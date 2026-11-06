-- Tested by running in the Azure portal query editor on 6 Nov 2026
-- (January returns one row; December raises error 50001 because data stops at September)
CREATE OR ALTER PROCEDURE gold.usp_monthly_kpi
    @year  INT,
    @month INT
AS
BEGIN
    SET NOCOUNT ON;

    IF NOT EXISTS (SELECT 1 FROM gold.vw_monthly_kpi WHERE [year] = @year AND [month] = @month)
        THROW 50001, 'No data for the requested year and month.', 1;

    SELECT *
    FROM gold.vw_monthly_kpi
    WHERE [year] = @year AND [month] = @month;
END;
