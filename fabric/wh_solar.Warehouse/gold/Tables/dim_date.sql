CREATE TABLE [gold].[dim_date] (
    [date]       DATE         NULL,
    [date_key]   INT          NULL,
    [year]       INT          NULL,
    [month]      INT          NULL,
    [month_name] VARCHAR (10) NULL,
    [weekday]    VARCHAR (10) NULL,
    [is_weekend] BIT          NULL
);


GO