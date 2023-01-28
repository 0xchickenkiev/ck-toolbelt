USE [Lake_Control]
GO

SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE PROCEDURE [Control].[Defragment]
(	
	@Server				sysname = NULL,
	@Database			sysname = NULL,		
	@Schema				sysname = NULL,			-- Optional - Focusses any compression changes to just one schema rather than all tables using the nominated partition scheme 
	@Table				sysname = NULL,			-- Optional - Nominates a single table which will be assessed for index defragmentation
	@WindowMinutes		int = NULL,				-- Optional - Nominates a total permissible elapsed period within which defragmentation can still be initiated in descending order of fragmentation
	@Online				bit = 1,				-- Optional - Run index rebuilds with online option
	@MinimumPages		int = 1000,				-- Optional - Minimum size of index in pages to be considered for defragmentation
	@RebuildPercent		int = 30,				-- Optional - Minimum fragmentation percentage for complete rebuild to be intiated
	@ReorganizePercent	int = 5,				-- Optional - Minimum fragmentation percentage for reorganize to be initiated
	@IgnorePageCompress	bit = 1,				-- Optional - Only consider partitions which are not PAGE compressed (assuming they were rebuilt to PAGE compress them)
	@LatestPartitions	int = NULL,				-- Optional - Sets a threshold for only considering the latest n partitions for each table for rebuild
	@DryRun				bit = 0					-- Optional - (0=FALSE/1=TRUE) - Allows the procedure to be dry run whereby it will report all of its intended actions but not actually execute them
)
AS

	SET NOCOUNT ON 

	DECLARE	@Partitions	TABLE	(	i int IDENTITY(1,1), 
									SchemaName varchar(128), 
									TableName varchar(128), 
									IndexName varchar(128),
									Partitioned bit, 
									PartitionNumber int, 
									Fragmentation numeric(5,2), 
									Action varchar(10), 
									ddl varchar(MAX),
									StartTime datetime2(3),
									EndTime	datetime2(3),
									Outcome varchar(10),
									ErrorMessage varchar(MAX)
								)

	DECLARE	@StartTime datetime2(0)

	DECLARE @i int

	DECLARE @SQL nvarchar(MAX)

	DECLARE @Alert			varchar(MAX),
			@Message		varchar(MAX),
			@Error			int,
			@ErrorMessage	varchar(MAX)

BEGIN

	SET @Database = ISNULL(@Database,db_name()) -- default to current database

	-- Ensure Server/Proxy/Database/Schema name parameters are not supplied in square brackets
	SET @Server = REPLACE(REPLACE(@Server,'[',''),']','')
	SET @Database = REPLACE(REPLACE(@Database,'[',''),']','')
	SET @Schema = REPLACE(REPLACE(@Schema,'[',''),']','')

	SET @StartTime = GETDATE()

	SET @Message = CHAR(13) + CHAR(10) + CONVERT(varchar,SYSDATETIME()) + ' - Analysing Fragmentation'

	SET @SQL = 'SELECT	SchemaName, TableName, IndexName, Partitioned, PartitionNumber, avg_fragmentation_in_percent Fragmentation,
						CASE WHEN avg_fragmentation_in_percent >= '+CONVERT(varchar,@RebuildPercent)+' THEN ''Rebuild'' WHEN avg_fragmentation_in_percent >= '+CONVERT(varchar,@ReorganizePercent)+' THEN ''Reorganize'' ELSE ''Pass'' END Action
				FROM	(	SELECT	s.Name SchemaName, t.name TableName, t.object_id TableId, i.name IndexName, i.index_id IndexId, CASE WHEN ps.data_space_id IS NULL THEN 0 ELSE 1 END Partitioned, p.partition_number PartitionNumber
							FROM	['+@Database+'].sys.schemas s
									INNER JOIN ['+@Database+'].sys.tables t ON (t.schema_id = s.schema_id)
									INNER JOIN ['+@Database+'].sys.indexes i ON (i.object_id = t.object_id AND i.type_desc <> ''HEAP'')
									INNER JOIN ['+@Database+'].sys.partitions p ON (p.object_id = i.object_id AND p.index_id = i.index_id)
									INNER JOIN ['+@Database+'].sys.allocation_units u ON (u.container_id = p.partition_id)
									LEFT JOIN ['+@Database+'].sys.partition_schemes ps ON (ps.data_space_id = i.data_space_id)
									LEFT JOIN ['+@Database+'].sys.partition_functions pf ON (pf.function_id = ps.function_id)
							WHERE	p.rows > 0' + CASE WHEN @Schema IS NOT NULL THEN '
									AND s.name = '''+@Schema+'''' ELSE '' END + CASE WHEN @Table IS NOT NULL THEN '
									AND t.name = '''+@Table+'''' ELSE '' END + CASE WHEN @IgnorePageCompress = 1 THEN '
									AND p.data_compression_desc <> ''PAGE''' ELSE '' END + CASE WHEN @LatestPartitions IS NOT NULL THEN '
									AND p.partition_number > ISNULL(pf.fanout,1) - '+CONVERT(varchar,@LatestPartitions) ELSE '' END + '
							GROUP BY s.Name, t.name, t.object_id, i.index_id, i.name, ps.data_space_id, p.partition_number
							HAVING SUM(u.data_pages) > '+CONVERT(varchar,@MinimumPages)+'
						) t
						OUTER APPLY	(SELECT avg_fragmentation_in_percent, alloc_unit_type_desc FROM sys.dm_db_index_physical_stats (db_id('''+@Database+'''), t.TableId, t.IndexId,t.PartitionNumber, DEFAULT) WHERE alloc_unit_type_desc = ''IN_ROW_DATA'') s'

	SET @SQL = 'SELECT	SchemaName, TableName, IndexName, Partitioned, PartitionNumber, Fragmentation, Action,
						''ALTER INDEX [''+IndexName+''] ON ['+@Database+'].[''+SchemaName+''].[''+TableName+'']''
							+ '' /* ''+ CONVERT(varchar,Fragmentation)+''% */''
							+ UPPER(Action) 
							+ CASE Partitioned WHEN 1 THEN '' PARTITION = ''+ CONVERT(varchar,PartitionNumber) ELSE '''' END
							+ '' WITH (SORT_IN_TEMPDB = ON'''+ CASE @Online WHEN 1 THEN '
							+ CASE Action WHEN ''REBUILD'' THEN '' , ONLINE = ON'' ELSE '''' END' ELSE '' END + ' 
							+'')'' ddl
				FROM	'+Control.RemoteQuery(@SQL,@Server,DEFAULT)+' x
				WHERE	Action IN (''Rebuild'', ''Reorganize'')
				ORDER BY Fragmentation'

	IF @DryRun = 1 SELECT @SQL

	INSERT	@Partitions (SchemaName, TableName, IndexName, Partitioned, PartitionNumber, Fragmentation, Action, ddl) EXECUTE (@SQL)
	SET @i = @@ROWCOUNT

	IF @DryRun = 1 SELECT * FROM @Partitions

	SET @Message = @Message + CHAR(13) + CHAR(10) + CONVERT(varchar,SYSDATETIME()) + ' - Defragmenting:'
	WHILE @i > 0 AND @DryRun = 0
	BEGIN
		SET	@Error = NULL
		SET	@ErrorMessage = NULL
		IF DATEDIFF(MINUTE,@StartTime,GETDATE()) < ISNULL(@WindowMinutes,9999)
			BEGIN
				UPDATE @Partitions SET StartTime = SYSDATETIME() WHERE i = @i
				SELECT @SQL = ddl FROM @Partitions WHERE i = @i
				IF ISNULL(@Server,@@SERVERNAME) <> @@SERVERNAME SET @SQL = 'EXEC('''+REPLACE(@SQL,'''','''''')+''') AT ['+@Server+']'
				BEGIN TRY
					EXEC sp_executesql @SQL
				END TRY
				BEGIN CATCH
					SET	@Error = ERROR_NUMBER()
					SET	@ErrorMessage = ERROR_MESSAGE()
				END CATCH
				UPDATE @Partitions SET EndTime = SYSDATETIME(), Outcome = CASE WHEN @Error IS NULL THEN 'Complete' ELSE 'Failure' END, ErrorMessage = CONVERT(varchar,@Error) + ' - ' + @ErrorMessage WHERE i = @i
			END
		ELSE
			UPDATE @Partitions SET StartTime = SYSDATETIME(), EndTime = SYSDATETIME(), Outcome = 'OutOfTime' WHERE i = @i

		SELECT @Message = @Message + CHAR(13) + CHAR(10) + CHAR(9) 
						+ CONVERT(varchar,Fragmentation) + '%' + CHAR(9) 
						+ Action + ' : ' + CHAR(9) 
						+ Outcome + CHAR(9) 
						+ '(' + CONVERT(varchar,StartTime) + ' - ' + CONVERT(varchar,EndTime) + ')' + CHAR(9) 
						+ '[' + SchemaName + '].[' + TableName + '].[' + IndexName + ']' + CASE Partitioned WHEN 1 THEN ' (Partition '+CONVERT(varchar,PartitionNumber)+')' ELSE '' END 
						+ CASE WHEN ErrorMessage IS NULL THEN '' ELSE CHAR(13) + CHAR(10) + CHAR(9) + CHAR(9) + CHAR(9) + CHAR(9) + CHAR(9) + ErrorMessage END
		FROM	@Partitions 
		WHERE	i = @i
		SET @i = @i - 1
	END
	SET @Alert = @@SERVERNAME + ' - ' + OBJECT_NAME(@@PROCID) + ' Finished for ['+ISNULL(@Database,DB_NAME(DB_ID()))+']'+ISNULL('.['+@Schema+']','')
	SET @Message = ISNULL(@Database,DB_NAME(DB_ID())) + CHAR(13) + CHAR(10)
						+ CHAR(13) + CHAR(10) + 'Supplied parameters: '
						+ CHAR(13) + CHAR(10) + CHAR(9) + '@Server = ' + ISNULL('''' + @Server + '''','NULL')
						+ CHAR(13) + CHAR(10) + CHAR(9) + '@Database = ' + ISNULL('''' + @Database + '''','NULL')
						+ CHAR(13) + CHAR(10) + CHAR(9) + '@Schema = ' + ISNULL('''' + @Schema + '''','NULL')
						+ CHAR(13) + CHAR(10) + CHAR(9) + '@Table = ' + ISNULL('''' + @Table + '''','NULL')
						+ CHAR(13) + CHAR(10) + CHAR(9) + '@WindowMinutes = ' + ISNULL(CONVERT(varchar,@WindowMinutes),'NULL')
						+ CHAR(13) + CHAR(10) + CHAR(9) + '@Online = ' + CONVERT(varchar,@Online)
						+ CHAR(13) + CHAR(10) + CHAR(9) + '@MinimumPages = ' + CONVERT(varchar,@MinimumPages)
						+ CHAR(13) + CHAR(10) + CHAR(9) + '@RebuildPercent = ' + CONVERT(varchar,@RebuildPercent)
						+ CHAR(13) + CHAR(10) + CHAR(9) + '@ReorganizePercent = ' + CONVERT(varchar,@ReorganizePercent)
						+ CHAR(13) + CHAR(10) + CHAR(9) + '@IgnorePageCompress = ' + CONVERT(varchar,@IgnorePageCompress)
						+ CHAR(13) + CHAR(10) + CHAR(9) + '@LatestPartitions = ' + ISNULL(CONVERT(varchar,@LatestPartitions),'NULL')
						+ CHAR(13) + CHAR(10) + @Message
						+ CHAR(13) + CHAR(10) + CONVERT(varchar,SYSDATETIME()) + ' - Finished'

	IF @DryRun = 0 AND ISNULL(@WindowMinutes,1) > 0
		EXEC [msdb].dbo.sp_send_dbmail
			@profile_name = 'mt-itp-smtp',
			@recipients = 'some@emailaddress.com',
			@body = @Message,
			@subject = @Alert

	SELECT * FROM @Partitions order by i DESC

END
GO


