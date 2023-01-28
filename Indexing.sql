USE [Lake_Control]
GO

SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE PROCEDURE [Control].[Indexing]
(	
	@Server			sysname = NULL,
	@Database		sysname = NULL,		
	@Schema			sysname,					-- Madatory - Limits any index management to a single schema
	@Table			sysname = NULL,				-- Optional - Focusses index management to just one table rather than all tables in the nominated schema 
	@Form			char(3) = 'LAK',			-- Optional - Specifies the form of the subject table(s) and consequently the indexing strategy. e.g. 'LAK' = standard Lake format, 'PUB' = CLUSTERED and DateFrom NONCLUSTERED only, NON = Clustered only, Anything else, drop all indexes
	@Scheme			sysname = NULL,				-- Optional - Specifies a new/changed partition scheme to be used for index creation. If not specified, the current scheme (if one exists) is maintained, otherwise no partitioning
	@PartitionKey	sysname = NULL,				-- Optional - Specifies a partition key column to be used for index target. If not specified, then partiton key rmaind unchanged or is created without partitioning 
	@ForceRecreate	bit = 0,					-- Optional - Forces a full recreate of all indexes (value = 1) even if no changes are required for defragmentation purposes etc.
	@RowPartitions	int = 2,						-- Optional - Sets the boundary between PAGE and ROW compression in the partitions, i.e. 0 = PAGE compression for all partitions, 2= ROW compression for last 2 partitions and PAGE for all others, etc. NULL = do not perform any compression changes
	@PrimaryKey		Control.NameList READONLY,	-- Optional - Focusses any compression changes to just one table rather than all tables using the nominated partition scheme 
	@DryRun			bit = 0						-- Optional - 1 = execute process as a dry run, with full logging to check gtenerated SQL and From/To dates without actually running any code, 0 = run as normal
)
AS

	DECLARE @tTables			TABLE (i int IDENTITY(1,1), TableName varchar(128), ObjectId int, PartitionScheme varchar(128))
	DECLARE	@tDiffIndexes		TABLE (i int IDENTITY(1,1), Src char(1), TableName varchar(128), IndexName varchar(128), IndexType varchar(60), UniqueIndex bit, PartitionScheme varchar(128), PartitionKey varchar(128), Partitions int, ColumnNames varchar(1000), Columns tinyint, IncludeNames varchar(1000), Includes tinyint)
	DECLARE	@tIndexes			TABLE (i int IDENTITY(1,1), Src char(1) DEFAULT 'T', TableName varchar(128), IndexName varchar(128), IndexType varchar(60), UniqueIndex bit, PartitionScheme varchar(128) NULL, PartitionKey sysname NULL, Partitions int NULL, ColumnName varchar(128), ColumnOrder int, Additional smallint, Columns tinyint)
	DECLARE	@tScript			TABLE (i int IDENTITY(1,1), TableName varchar(128), IndexName varchar(128), Seq int, DDL varchar(MAX))

	DECLARE @Partitions int

	DECLARE	@i int

	DECLARE	@SQL nvarchar(MAX)

BEGIN

	SET NOCOUNT ON 

	SET @SQL = 'SELECT	f.fanout
				FROM	['+ISNULL(@Database,db_name())+'].sys.partition_schemes s
						INNER JOIN ['+ISNULL(@Database,db_name())+'].sys.partition_functions f ON (f.function_id = s.function_id)
				WHERE	s.name = '''+@Scheme+''''
	SET @SQL = 'SELECT @Partitions = fanout FROM '+Control.RemoteQuery(@SQL,@Server,DEFAULT)+' x'

	EXEC dbo.sp_executesql  @SQL, N'@Partitions int OUTPUT', @Partitions=@Partitions OUTPUT


	INSERT @tIndexes (TableName, IndexName, IndexType, UniqueIndex, PartitionScheme, PartitionKey, ColumnName, ColumnOrder, Additional, Columns)
	EXEC Control.IndexBlueprint @Server, @Database, @Schema, DEFAULT, @Table, @Form, @Scheme, @PartitionKey, @PrimaryKey

	SET @SQL = 'SELECT	t.name TableName, 
						i.name IndexName, 
						i.type_desc IndexType,
						i.is_unique UniqueIndex,
						p.name PartitionScheme, 
						MAX(CASE c.partition_ordinal WHEN 0 THEN NULL ELSE l.name END) OVER (PARTITION BY t.Name) PartitionKey,
						f.fanout Partitions,
						l.name ColumnName, 
						c.key_ordinal ColumnOrder, 
						CASE WHEN c.is_included_column = 1 THEN 0 WHEN c.is_descending_key = 1 THEN -1 ELSE 1 END Additional,
						ISNULL(COUNT(*) OVER (PARTITION BY t.Name, i.Name),0) Columns
				FROM	['+ISNULL(@Database,db_name())+'].sys.tables t
						INNER JOIN ['+ISNULL(@Database,db_name())+'].sys.schemas s ON (s.schema_id = t.schema_id)
						INNER JOIN ['+ISNULL(@Database,db_name())+'].sys.indexes i ON (i.object_id = t.object_id)
						INNER JOIN ['+ISNULL(@Database,db_name())+'].sys.index_columns c ON (c.object_id = i.object_id AND c.index_id = i.index_id)
						INNER JOIN ['+ISNULL(@Database,db_name())+'].sys.columns l ON (l.object_id = c.object_id AND l.column_id = c.column_id)
						LEFT JOIN ['+ISNULL(@Database,db_name())+'].sys.partition_schemes p ON (p.data_space_id = i.data_space_id)
						LEFT JOIN ['+ISNULL(@Database,db_name())+'].sys.partition_functions f ON (f.function_id = p.function_id)
						WHERE	s.name = '''+@Schema+'''' + CASE WHEN @Table IS NOT NULL THEN '
								AND t.name = '''+@Table+'''' ELSE '' END+'
								AND t.name NOT IN (''__$Time'', ''__$Complete'')
								AND c.key_ordinal > 0 -- if partition key is not explicitly included in index, it is added implicitly as key_ordinal 0 - want to exclude that form comparing defined index structure vs blueprint
			'
	SET @SQL = 'SELECT ''C'' Src, * FROM '+Control.RemoteQuery(@SQL,@Server,DEFAULT)+' x ORDER BY TableName, IndexName, ColumnOrder'
	INSERT @tIndexes EXECUTE (@SQL)

	IF @DryRun = 1 SELECT * FROM @tIndexes ORDER BY TableName, IndexName, ColumnOrder, Src
						
	;WITH
	conc AS (	SELECT	Src,
						TableName, 
						IndexName,
						IndexType,
						UniqueIndex,
						PartitionScheme, 
						PartitionKey,
						Partitions,
						CONVERT(VARCHAR(1000), CASE Additional WHEN 0 THEN '' WHEN -1 THEN '['+ColumnName+'] DESC' ELSE '['+ColumnName+'] ASC' END) ColumnNames, 
						CASE Additional WHEN 0 THEN 0 ELSE 1 END Columns, 
						CONVERT(VARCHAR(1000), CASE Additional WHEN 0 THEN '['+ColumnName+']' ELSE '' END) IncludeNames, 
						CASE Additional WHEN 0 THEN 1 ELSE 0 END Includes, 
						i, i iRoot, 1 iDiff, 1 lev 
				FROM	@tIndexes
				UNION ALL
				SELECT	a.Src,
						a.TableName, 
						a.IndexName, 
						a.IndexType,
						a.UniqueIndex,
						a.PartitionScheme, 
						a.PartitionKey,
						a.Partitions,
						CONVERT(VARCHAR(1000),a.ColumnNames + CASE b.Additional 
																	WHEN 0 THEN '' 
																	WHEN -1 THEN CASE a.ColumnNames WHEN '' THEN '' ELSE ', ' END + '['+b.ColumnName+'] DESC' 
																	ELSE CASE a.ColumnNames WHEN '' THEN '' ELSE ', ' END + '['+b.ColumnName+'] ASC' 
																END) ColumnNames, 
						a.Columns + CASE b.Additional WHEN 0 THEN 0 ELSE 1 END Columns, 
						CONVERT(VARCHAR(1000),a.IncludeNames + CASE b.Additional 
																	WHEN 0 THEN CASE a.IncludeNames WHEN '' THEN '' ELSE ', ' END + '['+b.ColumnName+']' 
																	ELSE '' 
																END) IncludeNames, 
						a.Includes + CASE b.Additional WHEN 0 THEN 1 ELSE 0 END Includes, 
						b.i, a.iRoot, b.i - a.iRoot + 1 iDiff, a.lev+1 lev 
				FROM	conc a
						INNER JOIN @tIndexes b ON (b.Src = a.Src AND b.TableName = a.TableName AND b.IndexName = a.IndexName AND b.i > a.i)
				WHERE	b.i - a.iRoot + 1 = a.lev+1
				)
	INSERT INTO @tDiffIndexes (Src, TableName, IndexName, IndexType, UniqueIndex, PartitionScheme, PartitionKey, Partitions, ColumnNames, Columns, IncludeNames, Includes)
	SELECT	MAX(Src) Src, TableName, IndexName, IndexType, UniqueIndex, PartitionScheme, PartitionKey, MAX(MAX(Partitions)) OVER (PARTITION BY TableName) Partitions, ColumnNames, Columns, IncludeNames, Includes
	FROM	(	SELECT i.*, MAX(i.lev) OVER (PARTITION BY Src, TableName, IndexName) levMax 
				FROM conc i
			) x
	WHERE	lev = levMax
	GROUP BY TableName, IndexName, IndexType, UniqueIndex, PartitionScheme, PartitionKey, ColumnNames, Columns, IncludeNames, Includes, CASE @ForceRecreate WHEN 1 THEN Src ELSE '!' END
	HAVING COUNT(*) <> 2

	IF @DryRun = 1 SELECT * FROM @tDiffIndexes ORDER BY TableName, IndexName, Src

	INSERT INTO @tScript (TableName, IndexName, Seq, DDL)
		--SELECT	c.TableName, c.IndexName, 1 Seq, 'DROP INDEX ['+c.IndexName+'] ON ['+@Schema+'].['+c.TableName+']' DDL
		--FROM	@tDiffIndexes c
		--		LEFT JOIN @tDiffIndexes t ON (t.Src = 'T' AND t.TableName = c.TableName AND t.IndexType = c.IndexType AND t.ColumnNames = c.ColumnNames AND ISNULL(t.IncludeNames,'!') = ISNULL(c.IncludeNames,'!'))
		--WHERE	c.Src = 'C'
		--		AND c.IndexName LIKE '%(A)'
		--		AND c.IndexType = 'NONCLUSTERED'	
		--		AND t.TableName IS NULL	
		SELECT	TableName, IndexName, Seq, 'DROP INDEX ['+IndexName+'] ON ['+@Schema+'].['+TableName+']' DDL
		FROM	(	SELECT	1 Seq, c.TableName, c.IndexName
					FROM	@tDiffIndexes c
							LEFT JOIN @tDiffIndexes t ON (t.Src = 'T' AND t.TableName = c.TableName AND t.IndexType = c.IndexType AND t.ColumnNames = c.ColumnNames AND ISNULL(t.IncludeNames,'!') = ISNULL(c.IncludeNames,'!'))
					WHERE	c.Src = 'C'
							AND c.IndexName LIKE '%(A)'
							AND c.IndexType = 'NONCLUSTERED'
							AND t.TableName IS NULL
					UNION ALL
					SELECT	2 Seq, c.TableName, c.IndexName
					FROM	@tDiffIndexes c
							LEFT JOIN @tDiffIndexes t ON (t.Src = 'T' AND t.TableName = c.TableName AND t.IndexType = c.IndexType)
					WHERE	c.Src = 'C'
							AND c.IndexName LIKE '%(A)'
							AND c.IndexType = 'CLUSTERED'
							AND t.TableName IS NULL
				) x
		UNION ALL
		SELECT	TableName, IndexName, 3 Seq, 'exec sp_rename N'''+@Schema+'.'+TableName+'.'+CurrentIndexName+''', N'''+IndexName+''', N''INDEX''' DDL
		FROM	(	SELECT	t.TableName, t.IndexName, c.IndexName CurrentIndexName
					FROM	@tDiffIndexes t
							INNER JOIN @tDiffIndexes c ON (c.Src = 'C' AND c.TableName = t.TableName AND c.IndexType = t.IndexType AND c.IndexName <> t.IndexName)
					WHERE	t.Src = 'T'
							AND t.IndexType = 'CLUSTERED'
					UNION ALL		
					SELECT	t.TableName, t.IndexName, c.IndexName CurrentIndexName 
					FROM	@tDiffIndexes t
							INNER JOIN @tDiffIndexes c ON (c.Src = 'C' AND c.TableName = t.TableName AND c.IndexType = t.IndexType AND c.ColumnNames = t.ColumnNames AND ISNULL(c.IncludeNames,'!') = ISNULL(t.IncludeNames,'!') AND c.IndexName <> t.IndexName)
					WHERE	t.Src = 'T'
							AND t.IndexType = 'NONCLUSTERED'
				) x
		UNION ALL
		SELECT	TableName, IndexName, Seq, 
				'CREATE '+CASE UniqueIndex WHEN 1 THEN 'UNIQUE ' ELSE '' END+IndexType+' INDEX ['+IndexName+'] ON ['+@Schema+'].['+TableName+'] ('+ColumnNames+')'+
				CASE IncludeNames WHEN '' THEN '' ELSE ' INCLUDE ('+ IncludeNames + ')' END+
				' WITH (PAD_INDEX = OFF'+
						', STATISTICS_NORECOMPUTE = OFF'+
						', SORT_IN_TEMPDB = ON'+
						', DROP_EXISTING = '+DropExisting+
						', ONLINE = OFF'+
						', ALLOW_ROW_LOCKS = ON'+
						', ALLOW_PAGE_LOCKS = ON'+
					CASE WHEN PartitionKey IS NULL THEN ')' ELSE
						', DATA_COMPRESSION = ROW ON PARTITIONS (' + CONVERT(varchar,COALESCE(@Partitions,Partitions,0) - @RowPartitions + 1) + ' TO ' + CONVERT(varchar,COALESCE(@Partitions,Partitions,0)) + ')' +
						', DATA_COMPRESSION = PAGE ON PARTITIONS (1 TO ' + CONVERT(varchar,COALESCE(@Partitions,Partitions,0) - @RowPartitions) + ') )'+
						' ON ['+PartitionScheme+'] ('+PartitionKey+')'
					END DDL
		FROM	(	SELECT	4 Seq, t.TableName, t.IndexName, t.IndexType, t.UniqueIndex, t.PartitionScheme, t.PartitionKey, t.Partitions, t.ColumnNames, t.IncludeNames, CASE WHEN c.IndexName IS NULL THEN 'OFF' ELSE 'ON' END DropExisting
					FROM	@tDiffIndexes t
							LEFT JOIN @tDiffIndexes c ON (c.Src = 'C' AND c.TableName = t.TableName AND c.IndexType = t.IndexType)
					WHERE	t.Src = 'T'
							AND t.IndexType = 'CLUSTERED'
							AND (c.TableName IS NULL  OR c.ColumnNames <> t.ColumnNames OR ISNULL(c.PartitionScheme,'!') <> ISNULL(t.PartitionScheme,'!') OR ISNULL(c.PartitionKey,'!') <> ISNULL(t.PartitionKey,'!') OR c.UniqueIndex <> t.UniqueIndex)
					UNION ALL
					SELECT	5 Seq, t.TableName, t.IndexName, t.IndexType, t.UniqueIndex, t.PartitionScheme, t.PartitionKey, t.Partitions, t.ColumnNames, t.IncludeNames, CASE WHEN c.IndexName IS NULL THEN 'OFF' ELSE 'ON' END DropExisting
					FROM	@tDiffIndexes t
							LEFT JOIN @tDiffIndexes c ON (c.Src = 'C' AND c.TableName = t.TableName AND c.IndexType = t.IndexType AND c.ColumnNames = t.ColumnNames AND ISNULL(c.IncludeNames,'!') = ISNULL(t.IncludeNames,'!'))-- AND c.PartitionScheme = t.PartitionScheme)
					WHERE	t.Src = 'T'
							AND t.IndexType = 'NONCLUSTERED'
							AND (c.TableName IS NULL  OR c.ColumnNames <> t.ColumnNames OR ISNULL(c.IncludeNames,'!') <> ISNULL(t.IncludeNames,'!') OR ISNULL(c.PartitionScheme,'!') <> ISNULL(t.PartitionScheme,'!') OR ISNULL(c.PartitionKey,'!') <> ISNULL(t.PartitionKey,'!') )
				) x
		ORDER BY 1 DESC, 3 DESC, 2 DESC
		SET @i = @@ROWCOUNT

	IF @DryRun = 1 SELECT * FROM @tScript ORDER BY i

	WHILE @i > 0 AND @DryRun = 0
	BEGIN
		SELECT @SQL = DDL FROM @tScript WHERE i = @i
		SET @SQL = 'USE ['+ISNULL(@Database,db_name())+'] '+@SQL
		IF ISNULL(@Server,@@SERVERNAME) <> @@SERVERNAME SET @SQL = 'EXEC('''+REPLACE(@SQL,'''','''''')+''') AT ['+@Server+']'
		EXEC(@SQL)
		SET @i = @i - 1
	END

END
GO


