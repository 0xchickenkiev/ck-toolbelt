USE [Lake_Control]
GO

SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE PROCEDURE [Control].[Retention]
(	
	@Server				sysname = NULL,			-- Optional - Directs retention clear out to a server other than current one (default is current server) 
	@Database			sysname = NULL,			-- Optional - Directs retention clear out to a database other than current one (default is current database on current server or default database on any other server)		
	@Schema				sysname = NULL,			-- Optional - Directs retention clear out to a specific schema (default is dbo) 
	@Table				sysname = NULL,			-- Optional - Limits retention clear out to a single table (default is all tables in the scope set by the above parameters) 
	@RetentionDays		smallint,				-- Mandatory - Specifies the number of full days of data to be kept
	@DateBound			datetime2(3) = NULL,	-- Optional - Specifies an additional safe date past which retention clearout cannot go, irrespective of the value nominated in @Retention days 
	@RemovePartition	bit = 1,				-- Optional - Indicates whether truncated partitions before retention period will also be removed (merged)
	@DryRun				bit = 0					-- Optional - 1 = execute process as a dry run, with full logging to check gtenerated SQL and From/To dates without actually running any code, 0 = run as normal
)
AS

	DECLARE	@Metadata TABLE (TableName sysname, DBMS varchar(10), CompatibilityLevel tinyint, Form char(3), OriginTableName sysname NULL, OriginSchemaName sysname NULL, PartitionFunction sysname NULL, PartitionKey sysname NULL, DDLDate datetime2(3), DMLDate datetime2(3), RetentionDate datetime2(3), LSN binary(10), ColumnOrdinal int, ColumnName sysname, ColumnType sysname, ColumnLength smallint, ColumnScale tinyint, ColumnPrecision tinyint, ColumnCollation sysname NULL, KeyOrdinal int)
	DECLARE @Functions TABLE (i int IDENTITY(1,1), PartitionFunction varchar(128), RetentionPartitionNumber int)
	DECLARE @Tables TABLE (i int IDENTITY(1,1), TableName varchar(128), PartitionFunction varchar(128), RetentionPartitionNumber int, SQL varchar(MAX))

	DECLARE @RetentionDate datetime2(3)

	DECLARE @PartitionFunction sysname

	DECLARE	@i				int,
			@SQL			nvarchar(MAX)

	SET NOCOUNT ON 
	
BEGIN
	-- Collect metadata for table(s) nominated by parameters
	INSERT @Metadata (TableName, DBMS, CompatibilityLevel, Form, OriginTableName, OriginSchemaName, PartitionFunction, PartitionKey, DDLDate, DMLDate, RetentionDate, LSN, ColumnOrdinal, ColumnName, ColumnType, ColumnLength, ColumnScale, ColumnPrecision, ColumnCollation, KeyOrdinal) 
		EXEC Control.Metadata @Server = @Server, @Database = @Database, @Schema = @Schema, @Proxy = DEFAULT, @Table = @Table

	-- Derive retention date
	SELECT @RetentionDate = CONVERT(date,DATEADD(day,-@RetentionDays-1,DMLDate)) from @Metadata WHERE TableName = '__$Complete' AND ColumnName = 'CompleteDate'
	IF @RetentionDate > @DateBound SET @RetentionDate = @DateBound

	-- Construct a list of partition functions and the highest partition number to be cleared out up to retention date
	SET @SQL = ''
	SELECT	DISTINCT @SQL+='SELECT '''+PartitionFunction+''' PartitionFunction, ['+ISNULL(@Database,db_name(db_id()))+'].$PARTITION.'+PartitionFunction+'('''+CONVERT(varchar,@RetentionDate)+''') RetentionPartitionNumber ' 
	FROM	@Metadata 
	WHERE	PartitionFunction IS NOT NULL
	SET @SQL = 'SELECT * FROM '+Control.RemoteQuery(@SQL,@Server,DEFAULT)+' x'
	INSERT @Functions(PartitionFunction, RetentionPartitionNumber) EXEC (@SQL)
	IF @DryRun = 1 SELECT * FROM @Functions

	-- Use the partition function data to build a list of tables and the DDL required to truncate partitions before the retention period
	INSERT	@Tables 
	SELECT	DISTINCT m.TableName, m.PartitionFunction, f.RetentionPartitionNumber, 
			'TRUNCATE TABLE ['+ISNULL(@Database,db_name(db_id()))+'].['+ISNULL(@Schema,'dbo')+'].['+TableName+'] WITH (PARTITIONS (1 TO '+CONVERT(varchar,RetentionPartitionNumber - 1)+'))'
	FROM	@Metadata m 
			INNER JOIN @Functions f ON f.PartitionFunction = m.PartitionFunction
	WHERE	RetentionPartitionNumber > 1
	SET @i = @@ROWCOUNT
	IF @DryRun = 1 SELECT * FROM @Tables

	-- Loop through and run each SQL statement
	WHILE @i > 0 AND @DryRun = 0
		BEGIN
			SELECT @SQL = SQL FROM @Tables WHERE i = @i
			IF ISNULL(@Server,@@SERVERNAME) <> @@SERVERNAME SET @SQL = 'EXEC('''+REPLACE(@SQL,'''','''''')+''') AT ['+@Server+']'
			EXEC(@SQL)
			SET @i-=1
		END

	-- Remove partitions if the parameter has been set to activate this feature
	SELECT @i = COUNT(*) FROM @Functions
	WHILE @RemovePartition = 1 AND @Table IS NULL AND @i > 0 
		BEGIN
			SELECT @PartitionFunction = PartitionFunction FROM @Functions where i = @i
			EXEC Control.Partitioning @PartitionFunction, @FirstBoundary = @RetentionDate, @RowBoundary = NULL, @Trim=1, @Server = @Server, @Database = @Database, @Schema = @Schema, @DryRun = @DryRun
			SET @i-=1
		END

	-- If all tables have been processed in the schema (as opposed to a single table being processed in isolation) update the RetentionDate on the __$Complete for the schema
	IF @Table IS NULL EXEC Control.Complete @Server, @Database, @Schema, @RetentionDate = @RetentionDate
END
GO


