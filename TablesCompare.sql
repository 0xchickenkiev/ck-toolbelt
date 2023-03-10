USE [Lake_Control]
GO
/****** Object:  StoredProcedure [Control].[TablesCompare]    Script Date: 22/02/2023 10:52:19 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
ALTER PROCEDURE [Control].[TablesCompare]
(	
	@SourceServer		varchar(128) = @@SERVERNAME,	-- Optional - Specifies a linked server as the location for the source database, default is local server
	@SourceDatabase		varchar(128) = NULL,			-- Optional - Database as the source for table definition, default is current/default database
	@SourceSchema		varchar(128) = NULL,			-- Optional - Schema as the source for table definition, default is dbo schema
	@SourceProxy		varchar(128) = @@SERVERNAME,	-- Optional - Specifies a linked server as the Lake proxy for the source database to localise intensive metadata gethering activities
	@TargetServer		varchar(128) = @@SERVERNAME,	-- Optional - Specifies a linked server as the location for the target database, default is local server
	@TargetDatabase		varchar(128) = NULL,			-- Optional - Database as the target for table definition, default is current/default database
	@TargetSchema		varchar(128) = NULL,			-- Optional - Schema as the target for table definition, default is dbo schema
	@Form				char(3) = 'LAK',				-- Optional - Specifies the form of the target table(s) and consequently the metadata and indexing strategy.
	@Table				varchar(128) = NULL,			-- Optional - Focusses the data transfer on a specific table. The default is all tables present in both Source and Target schemas which conform to the types required by the task
	@Scheme				sysname = NULL,					-- Optional - Specifies a new/changed partition scheme to be used for index creation. If not specified, the current scheme (if one exists) is maintained, otherwise no partitioning
	@PartitionKey		varchar(128) = NULL,			-- Optional - Specifies a partition key column to be used for index target. If not specified, defaults to whatever is currently defined for target table or if new table, no partitioning 
	@AddMissingTables	bit = 1,						-- Optional - (1=Yes/0=No) Add any tables which are found in source schema but not already in target schema
	@PreserveTargetKey	bit = 1,						-- Optional - (1=Yes/0=No) Where a viable primary key is already defined on an existing target table, do not replace it if a different key is defined on the source table - allows us to select a more optimal unique key which isn't the primary key for the target 
	@DryRun				bit = 0							-- Optional - 1 = execute process as a dry run, with full logging to check gtenerated SQL and From/To dates without actually running any code, 0 = run as normal
)
AS

	DECLARE	@SourceMetadata TABLE (Origin char(6), TableName sysname, DBMS varchar(10), CompatibilityLevel tinyint, Form char(3), OriginTableName sysname NULL, OriginSchemaName sysname NULL, PartitionFunction sysname NULL, PartitionKey sysname NULL, DDLDate datetime2(3), DMLDate datetime2(7), RetentionDate datetime2(7), LSN binary(10), ColumnOrdinal int, ColumnName sysname, ColumnType sysname, ColumnLength int, ColumnScale tinyint, ColumnPrecision tinyint, ColumnCollation sysname NULL, KeyOrdinal int)
	DECLARE	@TargetMetadata TABLE (Origin char(6),TableName sysname, DBMS varchar(10), CompatibilityLevel tinyint, Form char(3), OriginTableName sysname NULL, OriginSchemaName sysname NULL, PartitionFunction sysname NULL, PartitionKey sysname NULL, DDLDate datetime2(3), DMLDate datetime2(7), RetentionDate datetime2(7), LSN binary(10), ColumnOrdinal int, ColumnName sysname, ColumnType sysname, ColumnLength int, ColumnScale tinyint, ColumnPrecision tinyint, ColumnCollation sysname NULL, KeyOrdinal int)
	DECLARE	@DifferenceMetadata TABLE (Origin char(6),TableName sysname, ColumnOrdinal int, ColumnName sysname, ColumnType sysname, ColumnLength int, ColumnScale tinyint, ColumnPrecision tinyint)

	DECLARE	@SourceDBMS					varchar(10),
			@SourceCompatibilityLevel	tinyint,
			@TargetDBMS					varchar(10),
			@TargetCompatibilityLevel	tinyint

	DECLARE	@PrimaryKey		Control.NameList

	DECLARE	@TableName		sysname,
			@TableStatus	char(1)

	DECLARE	@i				int,
			@SQL			nvarchar(MAX),
			@Error			int,
			@ErrorMessage	varchar(MAX),
			@Message		varchar(MAX),
			@Alert			varchar(MAX),
			@Failures		int

BEGIN

	SET NOCOUNT ON 

	SET @Scheme = ISNULL(@Scheme,@TargetSchema)
	
	BEGIN TRY
	-- Source and Target Table/Column Metadata
	INSERT @SourceMetadata (TableName, DBMS, CompatibilityLevel, Form, OriginTableName, OriginSchemaName, PartitionFunction, PartitionKey, DDLDate, DMLDate, RetentionDate, LSN, ColumnOrdinal, ColumnName, ColumnType, ColumnLength, ColumnScale, ColumnPrecision, ColumnCollation, KeyOrdinal) 
		EXEC Control.Metadata @Server = @SourceServer, @Database = @SourceDatabase, @Schema = @SourceSchema, @Proxy = @SourceProxy, @Table = @Table, @DMLDates = 0
	INSERT @TargetMetadata (TableName, DBMS, CompatibilityLevel, Form, OriginTableName, OriginSchemaName, PartitionFunction, PartitionKey, DDLDate, DMLDate, RetentionDate, LSN, ColumnOrdinal, ColumnName, ColumnType, ColumnLength, ColumnScale, ColumnPrecision, ColumnCollation, KeyOrdinal) 
		EXEC Control.Metadata @Server = @TargetServer, @Database = @TargetDatabase, @Schema = @TargetSchema, @Proxy = DEFAULT, @Table = @Table, @DMLDates = 0

		SELECT TOP 1 @SourceDBMS = DBMS, @SourceCompatibilityLevel = CompatibilityLevel FROM @SourceMetadata -- Just get DBMS from any row in metadata as all rows from same server should have same DBMS
		SELECT TOP 1 @TargetDBMS = DBMS, @TargetCompatibilityLevel = CompatibilityLevel FROM @TargetMetadata

		-- Construct a list of all differences between Source and Target 
		INSERT	@DifferenceMetadata
		SELECT	'Source', TableName, ColumnOrdinal, ColumnName, ColumnType, ColumnLength, ColumnScale, ColumnPrecision FROM @SourceMetadata WHERE TableName NOT LIKE '__$%'
		UNION ALL
		SELECT	'Target', TableName, ColumnOrdinal, ColumnName, ColumnType, ColumnLength, ColumnScale, ColumnPrecision FROM @TargetMetadata WHERE TableName NOT LIKE '__$%'
		
		SELECT * FROM @DifferenceMetadata ORDER BY 2,4,1

		-- Add an output that returns columns that are in source but not in target
		SELECT TableName, ColumnName 
		FROM 
			(
				SELECT TableName, ColumnName FROM @SourceMetadata WHERE TableName NOT LIKE '__$%'
				UNION ALL
				SELECT TableName, ColumnName FROM @TargetMetadata WHERE TableName NOT LIKE '__$%'
			) x
		GROUP BY TableName, ColumnName
		HAVING COUNT(*) <> 2
		ORDER BY 1, 2

		--Error logging

		SET @Failures = 0
		SET @Message = ''

		IF @Failures > 0
			BEGIN
				SET @Message = CONVERT(varchar,@Failures) + ' tables failed managment processing:' + CHAR(13) + CHAR(10) + @Message
				RAISERROR (@Message, 16, 1)
			END

	END TRY
	BEGIN CATCH 
		IF @@TRANCOUNT > 0 ROLLBACK
		;THROW 
	END CATCH
END
