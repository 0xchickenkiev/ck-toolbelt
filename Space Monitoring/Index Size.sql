-- 1691423 MB
-- 44922840	44905120	17720
SELECT 
    t.NAME AS TableName,
	i.name IndexName,
	p.partition_number,
	p.data_compression_desc,
    s.Name AS SchemaName,
	f.name AS FileGroup,
    p.rows AS RowCounts,
    SUM(a.total_pages) * 8 AS TotalSpaceKB, 
    SUM(a.used_pages) * 8 AS UsedSpaceKB, 
    (SUM(a.total_pages) - SUM(a.used_pages)) * 8 AS UnusedSpaceKB
FROM 
    sys.tables t
INNER JOIN      
    sys.indexes i ON t.OBJECT_ID = i.object_id
INNER JOIN 
    sys.partitions p ON i.object_id = p.OBJECT_ID AND i.index_id = p.index_id
INNER JOIN 
    sys.allocation_units a ON p.partition_id = a.container_id
LEFT JOIN 
    sys.filegroups f ON f.data_space_id = a.data_space_id
LEFT OUTER JOIN 
    sys.schemas s ON t.schema_id = s.schema_id
WHERE 
    t.NAME NOT LIKE 'dt%' 
    AND t.is_ms_shipped = 0
    AND i.OBJECT_ID > 255 
--	and s.name = 'cerillion'
--	and f.name = 'PRIMARY'
	and f.name = 'Lake_Default'
GROUP BY 
    t.Name, i.Name, p.partition_number, p.data_compression_desc, s.Name, f.name, p.Rows
ORDER BY 
    8 desc

