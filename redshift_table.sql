-- Create a table if it does not already exist with the specified table name
CREATE TABLE IF NOT EXISTS (table-name) (
    -- Define columns for the table
    clouds_percentage BIGINT,  -- Column to store cloudiness percentage as a bigint (integer)
    co_ords_lat FLOAT,  -- Column to store latitude coordinates as a floating-point number
    co_ords_lon FLOAT,  -- Column to store longitude coordinates as a floating-point number
    dt TIMESTAMP,  -- Column to store date and time as a timestamp
    feels_like FLOAT,  -- Column to store "feels like" temperature as a floating-point number
    humidity BIGINT,  -- Column to store humidity percentage as a bigint (integer)
    pressure BIGINT,  -- Column to store atmospheric pressure as a bigint (integer)
    temp FLOAT,  -- Column to store temperature as a floating-point number
    temp_max FLOAT,  -- Column to store maximum temperature as a floating-point number
    temp_min FLOAT,  -- Column to store minimum temperature as a floating-point number
    city VARCHAR(255),  -- Column to store city name as a variable-length string with a maximum length of 255 characters
    country VARCHAR(255),  -- Column to store country name as a variable-length string with a maximum length of 255 characters
    sunrise TIMESTAMP,  -- Column to store sunrise time as a timestamp
    sunset TIMESTAMP,  -- Column to store sunset time as a timestamp
    timezone TIMESTAMP,  -- Column to store timezone offset as a timestamp
    weather_description VARCHAR(255),  -- Column to store weather description as a variable-length string with a maximum length of 255 characters
    weather_main VARCHAR(255),  -- Column to store main weather condition as a variable-length string with a maximum length of 255 characters
    wind_deg BIGINT,  -- Column to store wind direction in degrees as a bigint (integer)
    wind_speed FLOAT  -- Column to store wind speed as a floating-point number
);
