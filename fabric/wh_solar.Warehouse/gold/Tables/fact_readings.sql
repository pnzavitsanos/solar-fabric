CREATE TABLE [gold].[fact_readings] (
    [date_key]           INT            NULL,
    [device_hash_id]     VARCHAR (8000) NULL,
    [station_hash_id]    VARCHAR (8000) NULL,
    [canonical_state_id] VARCHAR (8000) NULL,
    [ts]                 DATETIME2 (6)  NULL,
    [ac_power_kw]        FLOAT (53)     NULL,
    [dc_power_kw]        FLOAT (53)     NULL,
    [efficiency_pct]     FLOAT (53)     NULL,
    [energy_today_kwh]   FLOAT (53)     NULL,
    [energy_total_kwh]   FLOAT (53)     NULL,
    [temperature_c]      FLOAT (53)     NULL,
    [is_valid]           BIT            NULL,
    [source_file]        VARCHAR (8000) NULL
);


GO