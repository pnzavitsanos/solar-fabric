CREATE TABLE [gold].[fact_daily_energy] (
    [date_key]        INT            NULL,
    [station_hash_id] VARCHAR (8000) NULL,
    [device_hash_id]  VARCHAR (8000) NULL,
    [energy_kwh]      FLOAT (53)     NULL
);


GO