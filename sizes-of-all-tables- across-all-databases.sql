DECLARE @DatabaseName NVARCHAR(255), @DynamicSQL NVARCHAR(MAX);

-- Create a temporary table to store the results
CREATE TABLE #TableSizes (
    DatabaseName NVARCHAR(255),
    SchemaName NVARCHAR(255),
    TableName NVARCHAR(255),
    TableSizeMB DECIMAL(10, 2)
);

-- Declare a cursor for all databases
DECLARE db_cursor CURSOR FOR 
SELECT name 
FROM sys.databases 
WHERE state_desc = 'ONLINE' AND name NOT IN ('master', 'tempdb', 'model', 'msdb');

-- Open the cursor
OPEN db_cursor;

-- Fetch the first database
FETCH NEXT FROM db_cursor INTO @DatabaseName;

-- Iterate through all databases
WHILE @@FETCH_STATUS = 0
BEGIN
    -- Dynamic SQL to get table sizes for the current database
    SET @DynamicSQL = '
        USE [' + @DatabaseName + '];
        INSERT INTO #TableSizes (DatabaseName, SchemaName, TableName, TableSizeMB)
        SELECT 
            ''' + @DatabaseName + ''' AS DatabaseName,
            s.name AS SchemaName,
            t.name AS TableName,
            SUM(a.total_pages) * 8 / 1024.0 AS TableSizeMB
        FROM 
            sys.tables t
        INNER JOIN 
            sys.indexes i ON t.object_id = i.object_id
        INNER JOIN 
            sys.partitions p ON i.object_id = p.object_id AND i.index_id = p.index_id
        INNER JOIN 
            sys.allocation_units a ON p.partition_id = a.container_id
        LEFT OUTER JOIN 
            sys.schemas s ON t.schema_id = s.schema_id
        WHERE 
            t.type = ''U''
        GROUP BY 
            t.name, s.name
        ORDER BY 
            TableSizeMB DESC;
    ';

    -- Execute the dynamic SQL
    EXEC sp_executesql @DynamicSQL;

    -- Fetch the next database
    FETCH NEXT FROM db_cursor INTO @DatabaseName;
END;

-- Close and deallocate the cursor
CLOSE db_cursor;
DEALLOCATE db_cursor;

-- Select the results from the temporary table
SELECT * FROM #TableSizes ORDER BY DatabaseName, TableSizeMB DESC;

-- Drop the temporary table
DROP TABLE #TableSizes;
