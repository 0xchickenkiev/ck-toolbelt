DECLARE @command VARCHAR(5000)   
DECLARE @DBInfo TABLE   
( ServerName VARCHAR(100),   
DatabaseName VARCHAR(100),   
PhysicalFileName NVARCHAR(520),   
FileSizeMB DECIMAL(10,2),   
SpaceUsedMB DECIMAL(10,2),   
FreeSpaceMB DECIMAL(10,2), 
FreeSpacePct varchar(8) 
) 

SELECT @command = 'Use [' + '?' + '] SELECT   
@@servername as ServerName,   
' + '''' + '?' + '''' + ' AS DatabaseName   , filename 
    , convert(decimal(12,2),round(a.size/128.000,2)) as FileSizeMB 
    , convert(decimal(12,2),round(fileproperty(a.name,'+''''+'SpaceUsed'+''''+')/128.000,2)) as SpaceUsedMB 
    , convert(decimal(12,2),round((a.size-fileproperty(a.name,'+''''+'SpaceUsed'+''''+'))/128.000,2)) as FreeSpaceMB, 
    CAST(100 * (CAST (((a.size/128.0 -CAST(FILEPROPERTY(a.name,' + '''' + 'SpaceUsed' + '''' + ' ) AS int)/128.0)/(a.size/128.0)) AS decimal(4,2))) AS varchar(8)) + ' + '''' + '%' + '''' + ' AS FreeSpacePct 
from dbo.sysfiles a' 

INSERT INTO @DBInfo 
EXEC sp_MSForEachDB @command   

--SELECT * from @DBInfo
	
SELECT * from @DBInfo WHERE concat(PhysicalFileName,1,2) LIKE 'J:%' order by 4 desc