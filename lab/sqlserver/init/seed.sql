IF DB_ID('CRM') IS NULL CREATE DATABASE CRM;
GO
USE CRM;
GO
IF OBJECT_ID('dbo.Customer', 'U') IS NULL
BEGIN
    CREATE TABLE dbo.Customer (
        customer_id BIGINT NOT NULL PRIMARY KEY,
        name NVARCHAR(100) NOT NULL,
        status VARCHAR(20) NOT NULL,
        updated_at DATETIME2(6) NOT NULL,
        internal_note NVARCHAR(100) NULL
    );
END;
GO
MERGE dbo.Customer AS target
USING (VALUES
    (CAST(1 AS BIGINT), N'Ana', 'ACTIVE', CAST('2026-01-01T10:00:00' AS DATETIME2(6)), N'not contracted'),
    (CAST(2 AS BIGINT), N'Beto', 'ACTIVE', CAST('2026-01-01T10:00:00' AS DATETIME2(6)), N'not contracted'),
    (CAST(3 AS BIGINT), N'Caio', 'INACTIVE', CAST('2026-01-02T10:00:00' AS DATETIME2(6)), N'not contracted')
) AS source(customer_id, name, status, updated_at, internal_note)
ON target.customer_id = source.customer_id
WHEN NOT MATCHED THEN INSERT (customer_id, name, status, updated_at, internal_note)
VALUES (source.customer_id, source.name, source.status, source.updated_at, source.internal_note);
GO
UPDATE STATISTICS dbo.Customer WITH FULLSCAN;
GO
IF OBJECT_ID('dbo.Shape', 'U') IS NULL
BEGIN
    CREATE TABLE dbo.Shape (
        id BIGINT NOT NULL PRIMARY KEY,
        region VARCHAR(10) NOT NULL,
        amount DECIMAL(18,2) NULL,
        event_time DATETIME2(6) NULL,
        payload NVARCHAR(MAX) NULL
    );
    CREATE INDEX IX_Shape_region ON dbo.Shape(region) INCLUDE(amount);
    CREATE INDEX IX_Shape_event_time ON dbo.Shape(event_time);
END;
GO
MERGE dbo.Shape AS target
USING (VALUES
    (CAST(-100 AS BIGINT), 'A', CAST(-10.25 AS DECIMAL(18,2)), CAST('2026-01-01T00:00:00' AS DATETIME2(6)), N'negative'),
    (CAST(-1 AS BIGINT), 'A', CAST(NULL AS DECIMAL(18,2)), CAST(NULL AS DATETIME2(6)), N'nulls'),
    (CAST(1 AS BIGINT), 'A', CAST(0 AS DECIMAL(18,2)), CAST('2026-01-02T00:00:00' AS DATETIME2(6)), N'zero'),
    (CAST(2 AS BIGINT), 'A', CAST(0.01 AS DECIMAL(18,2)), CAST('2026-01-02T00:00:00' AS DATETIME2(6)), N'hot'),
    (CAST(3 AS BIGINT), 'A', CAST(1.00 AS DECIMAL(18,2)), CAST('2026-01-03T00:00:00' AS DATETIME2(6)), N'hot'),
    (CAST(1000000 AS BIGINT), 'A', CAST(2.00 AS DECIMAL(18,2)), CAST('2026-01-04T00:00:00' AS DATETIME2(6)), N'sparse'),
    (CAST(900000000 AS BIGINT), 'B', CAST(3.75 AS DECIMAL(18,2)), CAST(NULL AS DATETIME2(6)), N'sparse'),
    (CAST(900000001 AS BIGINT), 'C', CAST(NULL AS DECIMAL(18,2)), CAST('2026-01-05T00:00:00' AS DATETIME2(6)), N'new')
) AS source(id, region, amount, event_time, payload)
ON target.id = source.id
WHEN NOT MATCHED THEN INSERT (id, region, amount, event_time, payload)
VALUES (source.id, source.region, source.amount, source.event_time, source.payload);
GO
UPDATE STATISTICS dbo.Shape WITH FULLSCAN;
GO
IF OBJECT_ID('dbo.CompositeShape', 'U') IS NULL
    CREATE TABLE dbo.CompositeShape (group_id INT NOT NULL, item_id INT NOT NULL,
                                     payload VARCHAR(20) NOT NULL,
                                     CONSTRAINT PK_CompositeShape PRIMARY KEY(group_id,item_id));
GO
MERGE dbo.CompositeShape AS target
USING (VALUES (1,1,'a'),(1,2,'b'),(2,1,'c')) AS source(group_id,item_id,payload)
ON target.group_id=source.group_id AND target.item_id=source.item_id
WHEN NOT MATCHED THEN INSERT(group_id,item_id,payload)
VALUES(source.group_id,source.item_id,source.payload);
GO
UPDATE STATISTICS dbo.CompositeShape WITH FULLSCAN;
GO
IF OBJECT_ID('dbo.GuidShape', 'U') IS NULL
    CREATE TABLE dbo.GuidShape (id UNIQUEIDENTIFIER NOT NULL PRIMARY KEY, payload VARCHAR(20));
GO
MERGE dbo.GuidShape AS target
USING (VALUES
    (CONVERT(uniqueidentifier,'00000000-0000-0000-0000-000000000001'),'a'),
    (CONVERT(uniqueidentifier,'00000000-0000-0000-0000-000000000002'),'b'),
    (CONVERT(uniqueidentifier,'00000000-0000-0000-0000-000000000003'),'c')) AS source(id,payload)
ON target.id=source.id
WHEN NOT MATCHED THEN INSERT(id,payload) VALUES(source.id,source.payload);
GO
UPDATE STATISTICS dbo.GuidShape WITH FULLSCAN;
GO
IF OBJECT_ID('dbo.EmptyShape', 'U') IS NULL CREATE TABLE dbo.EmptyShape(id BIGINT NOT NULL PRIMARY KEY);
GO
IF OBJECT_ID('dbo.NoKeyShape', 'U') IS NULL
BEGIN
    CREATE TABLE dbo.NoKeyShape(n INT NOT NULL, payload VARCHAR(20));
    INSERT INTO dbo.NoKeyShape(n,payload) VALUES (1,'a'),(2,'b'),(3,'c');
END;
GO
IF OBJECT_ID('dbo.SkewNumeric', 'U') IS NULL
    CREATE TABLE dbo.SkewNumeric(id BIGINT NOT NULL PRIMARY KEY, payload VARCHAR(10));
GO
WITH numbers AS (SELECT TOP (199) ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS n
                 FROM sys.all_objects a CROSS JOIN sys.all_objects b)
INSERT INTO dbo.SkewNumeric(id,payload)
SELECT n,'dense' FROM numbers WHERE NOT EXISTS (SELECT 1 FROM dbo.SkewNumeric WHERE id=n);
GO
IF NOT EXISTS (SELECT 1 FROM dbo.SkewNumeric WHERE id=1000000000)
    INSERT INTO dbo.SkewNumeric(id,payload) VALUES (1000000000,'tail');
GO
UPDATE STATISTICS dbo.SkewNumeric WITH FULLSCAN;
GO
IF OBJECT_ID('dbo.SkewTemporal', 'U') IS NULL
BEGIN
    CREATE TABLE dbo.SkewTemporal(id INT NOT NULL, event_time DATETIME2(6) NOT NULL);
    CREATE UNIQUE INDEX UX_SkewTemporal_time ON dbo.SkewTemporal(event_time);
END;
GO
WITH numbers AS (SELECT TOP (199) ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS n
                 FROM sys.all_objects a CROSS JOIN sys.all_objects b)
INSERT INTO dbo.SkewTemporal(id,event_time)
SELECT n,DATEADD(SECOND,n,'2026-01-01T00:00:00') FROM numbers
WHERE NOT EXISTS (SELECT 1 FROM dbo.SkewTemporal WHERE id=n);
GO
IF NOT EXISTS (SELECT 1 FROM dbo.SkewTemporal WHERE id=1000000000)
    INSERT INTO dbo.SkewTemporal(id,event_time) VALUES (1000000000,'2027-01-01T00:00:00');
GO
UPDATE STATISTICS dbo.SkewTemporal WITH FULLSCAN;
GO
IF OBJECT_ID('dbo.HotShape', 'U') IS NULL
    CREATE TABLE dbo.HotShape(region VARCHAR(10) NOT NULL, id BIGINT NOT NULL,
                              payload VARCHAR(10),
                              CONSTRAINT PK_HotShape PRIMARY KEY(region,id));
GO
WITH numbers AS (SELECT TOP (200) ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS n
                 FROM sys.all_objects a CROSS JOIN sys.all_objects b)
INSERT INTO dbo.HotShape(region,id,payload)
SELECT CASE WHEN n<=180 THEN 'A' WHEN n<=190 THEN 'B' ELSE 'C' END,n,'value'
FROM numbers WHERE NOT EXISTS (SELECT 1 FROM dbo.HotShape WHERE id=n);
GO
UPDATE STATISTICS dbo.HotShape WITH FULLSCAN;
GO
IF OBJECT_ID('dbo.WideShape', 'U') IS NULL
    CREATE TABLE dbo.WideShape(id BIGINT NOT NULL PRIMARY KEY,
                               narrow VARCHAR(MAX), wide NVARCHAR(MAX),
                               blob VARBINARY(MAX), document XML, rv ROWVERSION);
GO
IF NOT EXISTS (SELECT 1 FROM dbo.WideShape WHERE id=1)
    INSERT INTO dbo.WideShape(id,narrow,wide,blob,document)
    VALUES(1,REPLICATE('x',4000),REPLICATE(N'á',4000),0x01020304,
           CONVERT(XML,'<root><item>one</item></root>'));
GO
UPDATE STATISTICS dbo.WideShape WITH FULLSCAN;
GO
IF OBJECT_ID('dbo.UniqueNoPk', 'U') IS NULL
BEGIN
    CREATE TABLE dbo.UniqueNoPk(id BIGINT NOT NULL, updated_at DATETIME2(6) NOT NULL,
                                payload VARCHAR(20));
    CREATE UNIQUE INDEX UX_UniqueNoPk_id ON dbo.UniqueNoPk(id);
END;
GO
IF NOT EXISTS (SELECT 1 FROM dbo.UniqueNoPk WHERE id=1)
    INSERT INTO dbo.UniqueNoPk(id,updated_at,payload)
    VALUES(1,'2026-01-01T00:00:00','a'),(2,'2026-01-02T00:00:00','b');
GO
UPDATE STATISTICS dbo.UniqueNoPk WITH FULLSCAN;
GO
IF OBJECT_ID('dbo.CompositeHot', 'U') IS NULL
    CREATE TABLE dbo.CompositeHot(group_id INT NOT NULL, item_id BIGINT NOT NULL,
                                  CONSTRAINT PK_CompositeHot PRIMARY KEY(group_id,item_id));
GO
WITH numbers AS (SELECT TOP (200) ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS n
                 FROM sys.all_objects a CROSS JOIN sys.all_objects b)
INSERT INTO dbo.CompositeHot(group_id,item_id)
SELECT CASE WHEN n<=180 THEN 1 WHEN n<=190 THEN 2 ELSE 3 END,n
FROM numbers WHERE NOT EXISTS (SELECT 1 FROM dbo.CompositeHot WHERE item_id=n);
GO
UPDATE STATISTICS dbo.CompositeHot WITH FULLSCAN;
GO
