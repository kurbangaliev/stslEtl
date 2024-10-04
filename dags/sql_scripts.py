sql_drop_tables = '''
    DROP TABLE IF EXISTS facts_departures;
    DROP TABLE IF EXISTS dim_routes;
    DROP TABLE IF EXISTS dim_stations;
    DROP TABLE IF EXISTS dim_customers;
    DROP TABLE IF EXISTS dim_cargo_types;
    DROP TABLE IF EXISTS dim_empty_types;
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
        destination_station_id SERIAL,
        CONSTRAINT fk_departure_station_id
          FOREIGN KEY(departure_station_id) 
            REFERENCES dim_stations(id),         
        CONSTRAINT fk_destination_station_id
          FOREIGN KEY(destination_station_id) 
            REFERENCES dim_stations(id)            
    );

    CREATE TABLE IF NOT EXISTS dim_customers (
        id SERIAL PRIMARY KEY,
        name VARCHAR(100)
    );
    
    CREATE TABLE IF NOT EXISTS dim_cargo_types (
        id SERIAL PRIMARY KEY,
        name VARCHAR(100)
    );
    
    CREATE TABLE IF NOT EXISTS dim_empty_types (
        id SERIAL PRIMARY KEY,
        name VARCHAR(10)
    );
    
    CREATE TABLE IF NOT EXISTS facts_departures (
        id SERIAL PRIMARY KEY,
        route_id INTEGER NOT NULL,
        transportation_sheet_id INTEGER NOT NULL,
        sender_id INTEGER NOT NULL,
        receiver_id INTEGER NOT NULL,
        real_wagon_loading INTEGER NOT NULL,
        real_train_weight_gross DECIMAL NOT NULL,
        real_train_wight_net DECIMAL NOT NULL,
        cargo_type_id INTEGER NOT NULL,
        empty_types_id INTEGER NOT NULL,
        down_time DECIMAL,
        axis_count INTEGER NOT NULL,
        end_pending_dttm DATE,        
        departure_dttm DATE,        
        calendar_stamp_dttm DATE,
        CONSTRAINT fk_route_id
          FOREIGN KEY(route_id) 
            REFERENCES dim_routes(id),      
        CONSTRAINT fk_sender_id
          FOREIGN KEY(sender_id) 
            REFERENCES dim_customers(id),        
        CONSTRAINT fk_receiver_id
          FOREIGN KEY(receiver_id) 
            REFERENCES dim_customers(id),                        
        CONSTRAINT fk_cargo_type_id
          FOREIGN KEY(cargo_type_id) 
            REFERENCES dim_cargo_types(id),
        CONSTRAINT fk_empty_types_id
          FOREIGN KEY(empty_types_id) 
            REFERENCES dim_empty_types(id)            
    );
'''

sql_fill_empty_types = '''
    INSERT INTO dim_empty_types (id, name) values (0, 'Порожний');
    INSERT INTO dim_empty_types (id, name) values (1, 'Груженный');
'''