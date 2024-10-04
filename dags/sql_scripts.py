sql_drop_tables = '''
    DROP TABLE IF EXISTS dim_routes;
    DROP TABLE IF EXISTS dim_stations;
    DROP TABLE IF EXISTS dim_customers;
'''

sql_create_tables = '''
    CREATE TABLE IF NOT EXISTS dim_stations (
        id SERIAL PRIMARY KEY,
        name VARCHAR(50),
        lon DECIMAL,
        lat DECIMAL
    );

    CREATE TABLE IF NOT EXISTS dim_routes (
        id SERIAL PRIMARY KEY,
        name VARCHAR(100),
        distance DECIMAL,
        standard_turn DECIMAL,
        departure_down_time_rate DECIMAL,
        arrival_delay_rate DECIMAL,
        wagon_count_loading INTEGER,
        train_weight_net DECIMAL,
        train_weight_gross DECIMAL,
        departure_station_id SERIAL,
        destination_station_id SERIAL
    );

    CREATE TABLE IF NOT EXISTS dim_customers (
        id SERIAL PRIMARY KEY,
        name VARCHAR(50)
    );
'''