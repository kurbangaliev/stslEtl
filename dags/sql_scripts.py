sql_drop_tables = '''
    DROP VIEW IF EXISTS facts_givings_view;
    DROP VIEW IF EXISTS facts_departures_view;
    DROP TABLE IF EXISTS facts_givings;
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
        end_pending_dttm TIMESTAMP NOT NULL,
        departure_dttm TIMESTAMP,
        calendar_stamp_dttm TIMESTAMP,
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
    
    CREATE TABLE IF NOT EXISTS facts_givings (
        id SERIAL PRIMARY KEY,
        giving_dttm TIMESTAMP NOT NULL,
        enrollment_dttm TIMESTAMP NOT NULL,
        down_time_train decimal,
        facts_departures_id INTEGER,
        transportation_sheet_id INTEGER NOT NULL,
        interval_giving DECIMAL,
        departure_dttm TIMESTAMP NOT NULL,
        end_pending_dttm TIMESTAMP NOT NULL,
        idle_train DECIMAL,
        travel_time DECIMAL,
        crew_down_time DECIMAL,
        crew_down_time_plan DECIMAL,
        intermediate_down_time DECIMAL          
    );
    
    CREATE OR REPLACE VIEW facts_departures_view
        AS
    SELECT fd.real_wagon_loading,
        fd.real_train_weight_gross,
        fd.real_train_wight_net,
        fd.down_time,
        fd.axis_count,
        fd.end_pending_dttm,
        fd.departure_dttm,
        fd.calendar_stamp_dttm,
        dr.name AS routename,
        dr.arrival_delay_rate AS routearrivaldelayrate,
        dr.departure_down_time_rate AS routedeparturedowntimerate,
        dr.distance AS routedistance,
        dr.standard_turn AS routestandardturn,
        dr.train_weight_gross AS routetrainweightgross,
        dr.train_weight_net AS routetrainweightnet,
        dr.wagon_count_loading AS routewagoncountloading,
        dprtst.name AS departurestationname,
        dprtst.lat AS departurestationlat,
        dprtst.lon AS departurestationlon,
        destst.name AS destinationstationname,
        destst.lat AS destinationstationlat,
        destst.lon AS destinationstationlon,
        ct.name AS cargotypename,
        cssender.name AS sendername,
        csreceiver.name AS receivername,
        et.name AS emptytypesname
    FROM facts_departures fd
        JOIN dim_routes dr ON dr.id = fd.route_id
        JOIN dim_stations dprtst ON dprtst.id = dr.departure_station_id
        JOIN dim_stations destst ON destst.id = dr.destination_station_id
        JOIN dim_cargo_types ct ON fd.cargo_type_id = ct.id
        JOIN dim_customers cssender ON cssender.id = fd.sender_id
        JOIN dim_customers csreceiver ON csreceiver.id = fd.receiver_id
        JOIN dim_empty_types et ON et.id = fd.empty_types_id;
        
    CREATE OR REPLACE VIEW facts_givings_view
        AS
    SELECT fg.crew_down_time AS factsgivingcrewdowntime,
        fg.crew_down_time_plan AS factsgivingcrewdowntimeplan,
        fg.departure_dttm AS factsgivingdeparturedate,
        fg.down_time_train AS factsgivingdowntimetrain,
        fg.end_pending_dttm AS factsgivingendpendingdate,
        fg.enrollment_dttm AS factsgivingenrollmentdate,
        fg.giving_dttm AS factsgivingdate,
        fg.idle_train AS factsgivingidletrain,
        fg.intermediate_down_time AS factsgivingintermediatedowntime,
        fg.interval_giving AS factsgivingintervalgiving,
        fg.travel_time AS factsgivingtraveltime,
        fd.real_wagon_loading,
        fd.real_train_weight_gross,
        fd.real_train_wight_net,
        fd.down_time,
        fd.axis_count,
        fd.end_pending_dttm,
        fd.departure_dttm,
        fd.calendar_stamp_dttm,
        dr.name AS routename,
        dr.arrival_delay_rate AS routearrivaldelayrate,
        dr.departure_down_time_rate AS routedeparturedowntimerate,
        dr.departure_station_id,
        dr.destination_station_id,
        dr.distance AS routedistance,
        dr.standard_turn AS routestandardturn,
        dr.train_weight_gross AS routetrainweightgross,
        dr.train_weight_net AS routetrainweightnet,
        dr.wagon_count_loading AS routewagoncountloading,
        dprtst.name AS departurestationname,
        dprtst.lat AS departurestationlat,
        dprtst.lon AS departurestationlon,
        destst.name AS destinationstationname,
        destst.lat AS destinationstationlat,
        destst.lon AS destinationstationlon,
        ct.name AS cargotypename,
        cssender.name AS sendername,
        csreceiver.name AS receivername,
        et.name AS emptytypesname
    FROM facts_givings fg
        JOIN facts_departures fd ON fg.facts_departures_id = fd.id
        JOIN dim_routes dr ON dr.id = fd.route_id
        JOIN dim_stations dprtst ON dprtst.id = dr.departure_station_id
        JOIN dim_stations destst ON destst.id = dr.destination_station_id
        JOIN dim_cargo_types ct ON fd.cargo_type_id = ct.id
        JOIN dim_customers cssender ON cssender.id = fd.sender_id
        JOIN dim_customers csreceiver ON csreceiver.id = fd.receiver_id
        JOIN dim_empty_types et ON et.id = fd.empty_types_id;        
'''

sql_fill_empty_types = '''
    INSERT INTO dim_empty_types (id, name) values (0, 'Порожний');
    INSERT INTO dim_empty_types (id, name) values (1, 'Груженный');
'''