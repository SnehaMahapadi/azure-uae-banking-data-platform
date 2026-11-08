-- Control table used by the Data Factory pipelines.
-- Rebuilt from the Azure SQL catalog (sys.columns, sys.indexes, sys.default_constraints)
-- on 9 November 2026, because the original create script was not saved.
-- Column types, nullability, the identity column, the primary key and the logged_at
-- default come from the live database. The identity seed and increment were not
-- captured, so IDENTITY(1,1) is assumed. Constraint names differ from the
-- system-generated names in the original.

IF SCHEMA_ID('ctl') IS NULL EXEC('CREATE SCHEMA ctl');
GO

IF OBJECT_ID('ctl.load_log', 'U') IS NULL
CREATE TABLE ctl.load_log (
    load_id       INT IDENTITY(1,1) NOT NULL,
    dataset       VARCHAR(50)       NOT NULL,
    file_name     VARCHAR(200)      NOT NULL,
    load_date     DATE              NOT NULL,
    rows_copied   BIGINT            NULL,
    status        VARCHAR(20)       NOT NULL,
    error_message VARCHAR(1000)     NULL,
    run_id        VARCHAR(100)      NULL,
    logged_at     DATETIME2         NOT NULL
        CONSTRAINT DF_load_log_logged_at DEFAULT (SYSUTCDATETIME()),
    CONSTRAINT PK_load_log PRIMARY KEY (load_id)
);
GO