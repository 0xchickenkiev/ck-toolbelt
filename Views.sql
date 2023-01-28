USE [Lake_Control]
GO

SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE PROCEDURE [Control].[Views]
(	
	@SourceServer		varchar(128) = @@SERVERNAME,	-- Optional - Specifies a linked server as the location for the source database, default is local server
	@SourceDatabase		varchar(128) = NULL,			-- Optional - Database as the source for table definition, default is current/default database
	@SourceSchema		varchar(128) = NULL,			-- Optional - Schema as the source for table definition, default is dbo schema
	@SourceProxy		varchar(128) = @@SERVERNAME,	-- Optional - Specifies a linked server as the Lake proxy for the source database to localise intensive metadata gethering activities
	@TargetServer		varchar(128) = @@SERVERNAME,	-- Optional - Specifies a linked server as the location for the target database, default is local server
	@TargetDatabase		varchar(128) = NULL,			-- Optional - Database as the target for table definition, default is current/default database
	@TargetSchema		varchar(128) = NULL,			-- Optional - Schema as the target for table definition, default is dbo schema
--	@Form				char(3) = 'LAK',				-- Optional - Specifies the form of the target table(s) and consequently the metadata and indexing strategy.
	@Table				varchar(128) = NULL,			-- Optional - Focusses the data transfer on a specific table. The default is all tables present in both Source and Target schemas which conform to the types required by the task
	@Scheme				sysname = NULL,					-- Optional - Specifies a new/changed partition scheme to be used for index creation. If not specified, the current scheme (if one exists) is maintained, otherwise no partitioning
--	@PartitionKey		varchar(128) = NULL,			-- Optional - Specifies a partition key column to be used for index target. If not specified, defaults to whatever is currently defined for target table or if new table, no partitioning 
--	@DateBound			datetime2(3) = NULL,			-- Optional - Applies an upper date bound to the source data to run upto but not beyond a defined point in time. Default of NULL runs up to current defined CompleteDate in source
--	@AddMissingTables	bit = 1,						-- Optional - (1=Yes/0=No) Add any tables which are found in source schema but not already in target schema
--	@PreserveTargetKey	bit = 1,						-- Optional - (1=Yes/0=No) Where a viable primary key is already defined on an existing target table, do not replace it if a different key is defined on the source table - allows us to select a more optimal unique key which isn't the primary key for the target 
	@DryRun				bit = 0							-- Optional - 1 = execute process as a dry run, with full logging to check generated SQL and From/To dates without actually running any code, 0 = run as normal
)
AS

	DECLARE	@SourceMetadata TABLE (TableName sysname, DBMS varchar(10), CompatibilityLevel tinyint, Form char(3), OriginTableName sysname NULL, OriginSchemaName sysname NULL, PartitionFunction sysname NULL, PartitionKey sysname NULL, DDLDate datetime2(3), DMLDate datetime2(7), RetentionDate datetime2(7), LSN binary(10), ColumnOrdinal int, ColumnName sysname, ColumnType sysname, ColumnLength smallint, ColumnScale tinyint, ColumnPrecision tinyint, ColumnCollation sysname NULL, KeyOrdinal int)

	DECLARE	@SourceDBMS					varchar(10),
			@SourceCompatibilityLevel	tinyint

	DECLARE	@TableName		sysname,
			@DB				nvarchar(255) = QUOTENAME(@TargetDatabase),
			@DBExec			nvarchar(max)


	DECLARE	@SQL			nvarchar(MAX),
			@Error			int,
			@ErrorMessage	varchar(MAX),
			@Message		varchar(MAX)


	BEGIN

	SET NOCOUNT ON 
	SET ANSI_WARNINGS OFF

	SET @Scheme = ISNULL(@Scheme,@TargetSchema)
	SET @DBExec = @DB + N'.sys.sp_executesql';
	
	-- Source and Target Table/Column Metadata
	INSERT @SourceMetadata (TableName, DBMS, CompatibilityLevel, Form, OriginTableName, OriginSchemaName, PartitionFunction, PartitionKey, DDLDate, DMLDate, RetentionDate, LSN, ColumnOrdinal, ColumnName, ColumnType, ColumnLength, ColumnScale, ColumnPrecision, ColumnCollation, KeyOrdinal) 
		EXEC Control.Metadata @Server = @SourceServer, @Database = @SourceDatabase, @Schema = @SourceSchema, @Proxy = @SourceProxy, @Table = @Table, @DMLDates = 0

		SELECT TOP 1 @SourceDBMS = DBMS, @SourceCompatibilityLevel = CompatibilityLevel FROM @SourceMetadata -- Just get DBMS from any row in metadata as all rows from same server should have same DBMS

			BEGIN
				
				BEGIN TRY

					------------------------------------------------------ CONSTRUCT DDL FOR TABLE  ----------------------------------------------------------------
					
							SET @SQL = ''
							SELECT	@SQL += N',[' + ColumnName + '] ' + CHAR(13)+CHAR(10)
							FROM	@SourceMetadata 
							WHERE	ColumnName NOT IN ('DateFrom','DateTo','RetentionDate','LoadedDate','CompleteDate')
							order by ColumnOrdinal
							SET @SQL = N'CREATE VIEW '+ @TargetSchema + '.' + @Table + ' AS SELECT ' 
								+ '[__$From]' + CHAR(13)+CHAR(10) + ',[__$To]' + CHAR(13)+CHAR(10) +',[__$Load]' + CHAR(13)+CHAR(10) +',[__$Action]' + CHAR(13)+CHAR(10) +',[__$Source]' 
								+ CHAR(13)+CHAR(10) + STUFF(@SQL, 1,0,'')+ ' FROM ' + @SourceDatabase + '.' + @SourceSchema + '.' + @Table + 
								' WHERE (__$To = CONVERT(datetime2(7), ''9999-12-31'')) AND __$Action <> ''D'';'
							
							IF @DryRun = 0 EXEC @DBExec @SQL ELSE SELECT @SQL

					------------------------------------------------------ Error Handling  ----------------------------------------------------------------

				END TRY

				BEGIN CATCH
					SET	@Error = ERROR_NUMBER()
					SET	@ErrorMessage = ERROR_MESSAGE()
					-- Add the failure to the overall failure status/message for all tables
					SET @Message = CONVERT(varchar,@Error) + ' - ' + @ErrorMessage
					RAISERROR (@Message, 16, 1)
				END CATCH

			END 

END
GO


