USE [Lake_Control]
GO

SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE PROCEDURE [Control].[Partitioning]
(	
	@Function		sysname,				-- Mandatory - any partition management in SQL Server will apply to all partitioned objects that use the partition function
	@Boundary		datetime2(3) = NULL,	-- Optional - Specifies a high boundary value (excluding 31/12/9999) to which partitions must be added or removed as defined by the other parameters
	@Grain			char(1) = NULL,			-- Optional - Specifies the grain at which partitons are to be created (NULL=Attempt to infer from existing partitions, D=Daily, Anything Else=Monthly)
	@RowBoundary	int = 4,				-- Optional - Sets the boundary between PAGE and ROW compression in the partitions, i.e. 0 = PAGE compression for all partitions, 2= ROW compression for last 2 partitions and PAGE for all others, etc. NULL = do not perform any compression changes
	@FirstBoundary	datetime2(3) = NULL,	-- Optional - Specifies a low boundary value from which partitions must be present - only has any effect in combination with @Trim or @GapFill to define the start of the range of boundaries between which should be filled or outside which should be trimmed
	@GapFill		bit = 0,				-- Optional - (0=FALSE/1=TRUE) - Indicates whether any gaps (based on 1 partiton per day) should be automatically filled between the current first partition and the outcome of the Boundary parameter above 
	@Trim			bit = 0,				-- Optional - (0=FALSE/1=TRUE) - Indicates whether any exisiting boundary balues beyond the Boundary parameter above should be dropped
	@Server			sysname = NULL,
	@Database		sysname = NULL,		
	@Schema			sysname = NULL,			-- Optional - Focusses any compression changes to just one schema rather than all tables using the nominated partition scheme 
	@Table			sysname = NULL,			-- Optional - Focusses any compression changes to just one table rather than all tables using the nominated partition scheme 
	@DryRun			bit = 0					-- Optional - (0=FALSE/1=TRUE) - Allows the procedure to be dry run whereby it will report all of its intended actions but not actually execute them
)
AS

	DECLARE	@Values TABLE (Value varchar(128))
	DECLARE	@Schemes TABLE (i int IDENTITY(1,1),Scheme sysname)
	DECLARE	@Boundaries TABLE (i int IDENTITY(1,1),Boundary sysname,Action varchar(10))
	DECLARE	@Compression TABLE (i int IDENTITY(1,1),SchemaName sysname, ObjectName sysname, IndexName sysname, PartitionNumber int, TargetCompression varchar(60))

	DECLARE	@SchemeCount int,
			@i int,
			@j int

	DECLARE @DefaultFilegroup bit
	DECLARE @FilegroupRoot sysname

	DECLARE @LowBoundary datetime2(3),
			@HighBoundary datetime2(3)

	DECLARE	@SQL nvarchar(MAX)

BEGIN

	SET NOCOUNT ON 

	-- Ensure Server/Proxy/Database/Schema name parameters are not supplied in square brackets
	SET @Server = REPLACE(REPLACE(@Server,'[',''),']','')
	SET @Database = REPLACE(REPLACE(@Database,'[',''),']','')
	SET @Schema = REPLACE(REPLACE(@Schema,'[',''),']','')
	-- Remove Frozen extension from database name if present to creat the root for filegroup names
	SET @FilegroupRoot = CASE WHEN @Database LIKE 'Lake%' THEN 'Lake' ELSE ISNULL(@Database,db_name()) END

	SET @SQL = 'SELECT @DefaultFilegroup = CASE COUNT(*) WHEN 0 THEN 1 ELSE 0 END FROM ['+ISNULL(@Database,db_name())+'].sys.filegroups WHERE Name LIKE '''+@FilegroupRoot+'\_'+@Function+'\_20[0-9][0-9][0-1][0-9]'' ESCAPE ''\'''
	EXEC dbo.sp_executesql @SQL, N'@DefaultFilegroup bit OUTPUT', @DefaultFilegroup=@DefaultFilegroup OUTPUT

	SET @SQL = 'SELECT	CONVERT(datetime2(3),v.value) value
				FROM	['+ISNULL(@Database,db_name())+'].sys.partition_functions f
						INNER JOIN ['+ISNULL(@Database,db_name())+'].sys.partition_range_values v ON v.function_id = f.function_id
				WHERE	f.name = '''+@Function+''''
	SET @SQL = 'SELECT * FROM '+Control.RemoteQuery(@SQL,@Server,DEFAULT)+' x'
	INSERT @Values EXECUTE (@SQL)

	-- If partition grain (daily or monthly) not explicitly specified, then try to infer from existing partitions
	IF @Grain IS NULL SELECT @Grain = CASE WHEN COUNT(*) <= 1 THEN 'M' WHEN DATEDIFF(day,MIN(value),MAX(value))/(COUNT(*) -1) > 20 THEN 'M' ELSE 'D' END FROM @Values WHERE value < CONVERT(DATETIME2(3),'9999-12-31')

	-- BASED ON THE SUPPLIED PARAMETERS, FINALISE THE RANGE OF BOUNDARIES OVER WHICH PARTITIONS ARE TO BE MANAGED
	SELECT	@LowBoundary = DATEADD(DAY,0,CONVERT(DATETIME2(3),ISNULL(MIN(Value),'2019-01-01'))),
			@HighBoundary =  DATEADD(DAY,-1,CONVERT(DATETIME2(3),ISNULL(MAX(Value),'2019-01-01')))
	FROM	@Values
	WHERE	Value < CONVERT(DATETIME2(3),'9999-12-31')

	IF @Boundary <> @HighBoundary SET @HighBoundary = @Boundary
	IF @FirstBoundary <> @LowBoundary SET @LowBoundary = @FirstBoundary

	IF @DryRun = 1 SELECT @Boundary Boundary, @FirstBoundary FirstBoundary, @LowBoundary LowBoundary, @HighBoundary HighBoundary

	-- IDENTIFY ALL PARTITION SCHEMES WHICH USE THE NOMINATED PARTITION FUNCTION
	SET @SQL = 'SELECT	s.Name
				FROM	['+ISNULL(@Database,db_name())+'].sys.partition_functions f
						INNER JOIN ['+ISNULL(@Database,db_name())+'].sys.partition_schemes s ON s.function_id = f.function_id
				WHERE	f.name = '''+@Function+''''
	SET @SQL = 'SELECT * FROM '+Control.RemoteQuery(@SQL,@Server,DEFAULT)+' x'
	INSERT @Schemes (Scheme) EXECUTE (@SQL)
	SET @SchemeCount = @@ROWCOUNT;
	
	IF @DryRun = 1 SELECT * FROM @Schemes;
	
	-- DERIVE A BOUNDARY BLUEPRINT AND COMPARE TO CURRENT AS-IS TO IDENTIFY BOUNDARIES TO BE INSERTED OR MERGED
	WITH iteration AS
		(	SELECT	CASE @Grain WHEN 'D' THEN @LowBoundary ELSE  DATEADD(DAY,1,CONVERT(datetime2(3),EOMONTH(@LowBoundary,-1))) END Boundary -- always start at begining of first period to create first empty partition
			UNION ALL
			SELECT	CASE @Grain WHEN 'D' THEN DATEADD(DAY,1,Boundary) ELSE DATEADD(MONTH,1,Boundary) END
			FROM	iteration
			WHERE	Boundary <= @HighBoundary			
		)
	INSERT INTO @Boundaries (Boundary, Action)
		SELECT Boundary, MAX(src) Action
		FROM	(	SELECT Boundary, 'Insert' src FROM iteration -- range of required partitions derived above based on date boundaries
					UNION ALL
					SELECT CONVERT(DATETIME2(3),'9999-12-31 23:59:59.999') Boundary, 'Insert' src -- last empty partition
					UNION ALL
					SELECT CONVERT(DATETIME2(3),'9999-12-31') Boundary, 'Insert' src -- current partition
					UNION ALL
					SELECT	CONVERT(DATETIME2(3),Value), 'Merge' src -- existing partitions
					FROM	@Values
				) b
		GROUP BY Boundary
		HAVING COUNT(*) <> 2
				AND	(	(MAX(src) = 'Insert' AND Boundary > @HighBoundary) -- always insert new high watermark partition irrespective of @Trim or @Gapfill parameters
						OR (MAX(src) = 'Insert' AND @GapFill = 1) -- only insert any other partitions if @GapFill parameter is true
						OR (MAX(src) = 'Merge' AND @Trim = 1) -- only remove (merge) partitions if @Trim parameter is true
					)
		ORDER BY DATEDIFF(day,'1900-01-01',Boundary) * CASE MAX(src) WHEN 'Insert' THEN -1 ELSE 1 END DESC
		OPTION	(maxrecursion 0)
		SET @i = @@ROWCOUNT

	IF @DryRun = 1 SELECT * FROM @Boundaries

	WHILE @i > 0
	BEGIN
		SET @SQL = ''
		IF (SELECT Action FROM  @Boundaries WHERE i = @i) = 'Insert'
			BEGIN
				-- We have to prime a Filegroup for every related partition scheme before we can add a new boundary. If filegroups already exist with names like <Function>__YYMM then assume one should exist for new boundary, otherwise revert to PRIMARY 
				SELECT	@SQL += 'ALTER PARTITION SCHEME '+ s.Scheme +' NEXT USED '+@FilegroupRoot+'_'+CASE WHEN Boundary = '9999-12-31 23:59:59.999' OR @DefaultFilegroup = 1 THEN 'Default' ELSE @Function+'_'+CONVERT(char(6),REPLACE(b.Boundary,'-','')) END+'; ' 
				FROM	@Schemes s, @Boundaries b 
				WHERE	b.i = @i
				-- Now we can add the new boundary
				SELECT @SQL += 'ALTER PARTITION FUNCTION '+ @Function +' () SPLIT RANGE (''' + Boundary + '''); ' FROM @Boundaries WHERE i = @i
			END
		ELSE
			SELECT @SQL += 'ALTER PARTITION FUNCTION '+ @Function +' () MERGE RANGE (''' + Boundary + ''');' FROM @Boundaries WHERE i = @i

		IF @Database IS NOT NULL SET @SQL = 'USE ['+@Database+'] '+@SQL
		IF ISNULL(@Server,@@SERVERNAME) <> @@SERVERNAME SET @SQL = 'EXEC('''+REPLACE(@SQL,'''','''''')+''') AT ['+@Server+']'

		IF @DryRun = 1 SELECT @SQL ELSE EXEC(@SQL)
		SET @i = @i - 1	
	END
	
	-- MANAGE COMPRESSION
	IF @RowBoundary IS NOT NULL
	BEGIN
		SET @SQL = 'SELECT	SchemaName,
							ObjectName, 
							IndexName, 
							PartitionNumber, 
							TargetCompression
					FROM	(	SELECT	c.name SchemaName,
										o.name ObjectName, 
										i.name IndexName, 
										p.partition_number PartitionNumber, 
										p.data_compression_desc 
										CurrentCompression, 
										CASE 
											WHEN p.partition_number = 1 THEN ''ROW'' -- empty first partition
											WHEN MAX(partition_number) OVER (PARTITION BY p.object_id, p.index_id) - '+CONVERT(varchar,@RowBoundary)+' >= p.partition_number THEN ''PAGE'' 
											ELSE ''ROW'' 
										END TargetCompression
								FROM	['+ISNULL(@Database,db_name())+'].sys.partition_functions f
										INNER JOIN ['+ISNULL(@Database,db_name())+'].sys.partition_schemes s ON (s.function_id = f.function_id)
										INNER JOIN ['+ISNULL(@Database,db_name())+'].sys.indexes i ON (i.data_space_id = s.data_space_id)
										INNER JOIN ['+ISNULL(@Database,db_name())+'].sys.objects o ON (o.object_id = i.object_id)
										INNER JOIN ['+ISNULL(@Database,db_name())+'].sys.schemas c ON (c.schema_id = o.schema_id)
										INNER JOIN ['+ISNULL(@Database,db_name())+'].sys.partitions p ON (p.object_id = i.object_id AND p.index_id = i.index_id)
								WHERE	i.type_desc NOT LIKE ''%COLUMNSTORE%''
										AND f.Name = '''+@Function+''''+CASE WHEN @Table IS NOT NULL THEN '
										AND o.name = '''+@Table+'''' ELSE '' END+CASE WHEN @Schema IS NOT NULL THEN '
										AND c.name = '''+@Schema+'''' ELSE '' END+'
							) x
					WHERE	TargetCompression <> CurrentCompression'
		SET @SQL = 'SELECT * FROM '+Control.RemoteQuery(@SQL,@Server,DEFAULT)+' x ORDER BY SchemaName DESC, ObjectName DESC, PartitionNumber DESC, IndexName DESC'
--select @SQL
		INSERT @Compression (SchemaName, ObjectName, IndexName, PartitionNumber, TargetCompression) EXECUTE (@SQL)
		SET @i = @@ROWCOUNT

		IF @DryRun = 1 SELECT * FROM @Compression

		WHILE @i > 0 AND @DryRun = 0
		BEGIN
			SELECT	@SQL = 'ALTER ' + CASE WHEN IndexName IS NULL THEN 'TABLE ' ELSE 'INDEX ['+ IndexName +'] ON ' END + 
							'['+ISNULL(@Database,db_name())+'].[' + SchemaName + '].[' + ObjectName + ']' + 
							' REBUILD PARTITION = ' + CONVERT(VARCHAR(20),PartitionNumber) + 
							' WITH (SORT_IN_TEMPDB = ON, DATA_COMPRESSION = ' + TargetCompression + ') ' 
			FROM	@Compression 
			WHERE	i = @i
			IF ISNULL(@Server,@@SERVERNAME) <> @@SERVERNAME SET @SQL = 'EXEC('''+REPLACE(@SQL,'''','''''')+''') AT ['+@Server+']'
			EXEC(@SQL)
			SET @i = @i - 1	
		END
	END

END
GO


