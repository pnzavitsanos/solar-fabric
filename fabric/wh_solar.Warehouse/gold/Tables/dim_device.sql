CREATE TABLE [gold].[dim_device] (
    [device_hash_id]  VARCHAR (8000) NULL,
    [station_hash_id] VARCHAR (8000) NULL,
    [name]            VARCHAR (8000) NULL,
    [device_model]    VARCHAR (8000) NULL,
    [max_power_kw]    FLOAT (53)     NULL,
    [vendor]          VARCHAR (8000) NULL
);


GO