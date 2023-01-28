USE [Lake_Control]
GO

SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE PROCEDURE [Control].[SafeDrop]
(	
	@DropTable			SYSNAME	 -- Specify the table that you would like to be dropped
)
AS

BEGIN

SET NOCOUNT ON 

	DECLARE	@RowCount		TABLE (Total INT)
	DECLARE	@Count			varchar(128),
			@SQL			nvarchar(128)

------------------------------------------- Counts number of rows in @DropTable and populates @RowCount with results---------------------------------

		BEGIN TRY
			SELECT @Count = 'SELECT COUNT (*) FROM ' + @DropTable + '';
			INSERT INTO @RowCount EXEC (@Count)
		END	TRY 
		BEGIN CATCH
			SELECT  
			 ERROR_NUMBER() AS ErrorNumber  
            ,ERROR_MESSAGE() AS ErrorMessage;
		END CATCH

------------------------------------------- If table has 0 rows, drop it. If it has more than 0, don't! ---------------------------------

	IF (SELECT * FROM @RowCount) = 0
		BEGIN
			SET @SQL = N'DROP TABLE ' + @DropTable + '';
			EXEC sp_executesql @SQL;
		END

	ELSE 
			PRINT 'Cannot drop table, ' + @DropTable + ' contains data.'
END;
GO


