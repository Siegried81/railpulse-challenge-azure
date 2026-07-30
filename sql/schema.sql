IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'stations')
CREATE TABLE stations (
    station_id           VARCHAR(20)   NOT NULL PRIMARY KEY,
    name                 VARCHAR(100)  NOT NULL,
    standard_name        VARCHAR(100),
    latitude              FLOAT,
    longitude             FLOAT,
    wheelchair_boarding  TINYINT,
    location_type        TINYINT
);

IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'vehicles')
CREATE TABLE vehicles (
    vehicle_id      VARCHAR(100)  NOT NULL PRIMARY KEY,
    vehicle_type    VARCHAR(10),
    direction       VARCHAR(50)
);

IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'liveboard_records')
CREATE TABLE liveboard_records (
    record_id       INT IDENTITY(1,1) PRIMARY KEY,
    station_id      VARCHAR(20)   NOT NULL FOREIGN KEY REFERENCES stations(station_id),
    vehicle_id      VARCHAR(100)  NOT NULL FOREIGN KEY REFERENCES vehicles(vehicle_id),
    platform        VARCHAR(10),
    scheduled_time  DATETIME      NOT NULL,
    delay_seconds   INT           DEFAULT 0,
    canceled        BIT           DEFAULT 0,
    pulled_at       DATETIME      NOT NULL DEFAULT GETUTCDATE(),
    CONSTRAINT UQ_liveboard_dedup UNIQUE (station_id, vehicle_id, scheduled_time, pulled_at)
);

CREATE INDEX IX_liveboard_station_time
    ON liveboard_records (station_id, scheduled_time);