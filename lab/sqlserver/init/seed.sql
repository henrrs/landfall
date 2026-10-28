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
