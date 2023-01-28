USE [Lake_Control]
GO

SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE PROCEDURE [Control].[AddFilegroup]
(	
	@Database		SYSNAME,		-- Specify the table that you would like to be
	@Schema			varchar(128),	-- The schema for the filegroup to be created for
	@Year			varchar(15),	-- The year for the filegroups to be created. Format = 2020
	@Month			varchar(15),	-- The month for the filegroup to be created. Format = 01, 02, 03 etc. 
	@DryRun			bit = 0			-- Optional - 1 = execute process as a dry run, with full logging to check generated SQL and From/To dates without actually running any code, 0 = run as normal

)

AS

BEGIN

SET NOCOUNT ON 
	
	DECLARE @YearMonth			varchar(20)		= @Year + @Month,
			@FileGroupName		varchar(40),
			@FileSize			varchar(26)		= 'SIZE = 8192KB',
			@FileGrowth			varchar(26)		= 'FILEGROWTH = 65536KB', 
			@LatestFilegroup	varchar(60),
			@Path				varchar(max)

	DECLARE @SQL				nvarchar(max),
			@Error				int,
			@ErrorMessage		varchar(MAX),
			@Message			varchar(MAX)


	SELECT @LatestFilegroup = (SELECT MAX(create_lsn) FROM sys.master_files where db_name(database_id) = @Database and name like '%' + @Schema + '%') -- Find the latest filegroup that has been created to copy file location and naming conventions
	
	SELECT @FileGroupName = (SELECT name FROM sys.master_files where db_name(database_id) = @Database and create_lsn = @LatestFilegroup) --Select the records needed using the lsn from the previous step

	SELECT @Path = (SELECT physical_name FROM sys.master_files where db_name(database_id) = @Database and create_lsn = @LatestFilegroup) -- Get the path that needs to be set for the new filegroup

	SET @FileGroupName = REPLACE(@FileGroupName, RIGHT(@FileGroupName, 6), '') + @YearMonth
	
	SET @Path = REPLACE(@Path, RIGHT(@Path, 10), '') + @YearMonth

				BEGIN
						
						BEGIN TRY

					------------------------------------------------------ CONSTRUCT DDL FOR FILEGROUP ----------------------------------------------------------------

							SET @SQL = N'ALTER DATABASE ' + '[' + @Database + ']' + ' ADD FILEGROUP ' + '[' + @FileGroupName + ']';
												
							IF @DryRun = 0 EXEC (@SQL) ELSE SELECT @SQL

					------------------------------------------------------ CONSTRUCT DDL FOR FILE ----------------------------------------------------------------

							SET @SQL = N'ALTER DATABASE ' + '[' + @Database + ']' + ' ADD FILE ( NAME = N''' + @FileGroupName + '''' + ',' + ' FILENAME = N''' + @Path + '.ndf' 
							+ ''' , ' +  @FileSize + ' , ' + @FileGrowth + ' ) ' + 'TO FILEGROUP ' + '[' + @FileGroupName + ']'
					
							IF @DryRun = 0 EXEC (@SQL) ELSE SELECT @SQL

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
END;
GO


