import json
import logging
import requests
import pathlib
import os
import shutil
import pandas as pd
import datetime
import threading
import glob
from dateutil.relativedelta import relativedelta
from sql_transform import Transform

logger = logging.getLogger(__name__)

class Loader:
    loader_begin_date = datetime.datetime(2021, 9, 1)

    @staticmethod
    def clear_data():
        pathlib.Path('data/').mkdir(parents=True, exist_ok=True)
        pathlib.Path('data/giving/').mkdir(parents=True, exist_ok=True)
        pathlib.Path('data/departure/').mkdir(parents=True, exist_ok=True)
        pathlib.Path('data/sql/').mkdir(parents=True, exist_ok=True)
        shutil.rmtree('data/')

    @staticmethod
    def get_departure_documents_period(begin_date, end_date, thread_num):
        api_departure_url = f"http://webapp.corp.darrail.com/GivingDepartureWebApp/api/etl/departureDocument/{begin_date.strftime("%d.%m.%Y")}/{end_date.strftime("%d.%m.%Y")}/"
        response = requests.get(api_departure_url)
        departure_documents = json.loads(response.text)
        if (len(departure_documents)) > 0:
            logger.info(f"get departure documents count {len(departure_documents)}")
            df = pd.json_normalize(departure_documents)
            df['SENDERNAME'] = df['SENDERNAME'].str.replace('"', '')
            df['RECEIVERNAME'] = df['RECEIVERNAME'].str.replace('"', '')
            df['ROUTENAME'] = df['ROUTENAME'].str.replace('"', '')
            df['ACTNUMBERDEPARTURE'] = df['ACTNUMBERDEPARTURE'].str.replace('"', '')
            df['DESCRIPTION'] = df['DESCRIPTION'].str.replace('"', '')
            df['CARGOTYPENAME'] = df['CARGOTYPENAME'].str.replace('"', '')
            df.to_csv(f"data/departure/departure_documents_{thread_num}.csv", index=False)

    @staticmethod
    def get_departure_documents():
        logger.info("get_departure_documents")
        pathlib.Path('data/departure/').mkdir(parents=True, exist_ok=True)

        begin_date = Loader.loader_begin_date
        index = 0
        threads = []

        while True:
            index += 1
            begin_date = begin_date + relativedelta(months=1)
            end_date = begin_date + relativedelta(months=1) - relativedelta(days=1)
            # Loader.get_departure_documents_period(begin_date, end_date, thread_num=1)
            thread = threading.Thread(target=Loader.get_departure_documents_period, args=(begin_date, end_date, index))
            threads.append(thread)
            thread.start()
            if begin_date > datetime.datetime.now():
                break

        for index, thread in enumerate(threads):
            logging.info("Main    : before joining thread %d.", index)
            thread.join()
            logging.info("Main    : thread %d done", index)

    @staticmethod
    def get_giving_documents_period(begin_date, end_date, thread_num):
        api_giving_url = f"http://webapp.corp.darrail.com/GivingDepartureWebApp/api/etl/givingDocument/{begin_date.strftime("%d.%m.%Y")}/{end_date.strftime("%d.%m.%Y")}/"
        response = requests.get(api_giving_url)
        giving_documents = json.loads(response.text)
        if (len(giving_documents)) > 0:
            logger.info(f"get departure documents count {len(giving_documents)}")
            df = pd.json_normalize(giving_documents)
            df.to_csv(f"data/giving/giving_documents_{thread_num}.csv", index=False)

    @staticmethod
    def get_giving_documents():
        logger.info("get_giving_documents")
        pathlib.Path('data/giving/').mkdir(parents=True, exist_ok=True)

        begin_date = Loader.loader_begin_date
        index = 0
        threads = []

        while True:
            index += 1
            begin_date = begin_date + relativedelta(months=1)
            end_date = begin_date + relativedelta(months=1) - relativedelta(days=1)

            thread = threading.Thread(target=Loader.get_giving_documents_period, args=(begin_date, end_date, index))
            threads.append(thread)
            thread.start()
            if begin_date > datetime.datetime.now():
                break

        for index, thread in enumerate(threads):
            logging.info("Main    : before joining thread %d.", index)
            thread.join()
            logging.info("Main    : thread %d done", index)

    @staticmethod
    def fill_total_documents():
        Loader.concat_documents(search_path='data/departure', mask='departure_documents*.csv',
                                output_file='data/departure_documents.csv')

        Loader.concat_documents(search_path='data/giving/', mask='giving_documents*.csv',
                                output_file='data/giving_documents.csv')

    @staticmethod
    def concat_documents(search_path, mask, output_file):
        all_files = glob.glob(os.path.join(search_path, mask))
        logger.info(f"all_files in {search_path} with mask {mask} count={len(all_files)}")
        documents = pd.concat((pd.read_csv(f) for f in all_files), ignore_index=True)
        documents.to_csv(output_file, index=False)

    @staticmethod
    def create_dims():
        pathlib.Path('data/sql/').mkdir(parents=True, exist_ok=True)
        # all_files = glob.glob(os.path.join('data/departure', "departure_documents*.csv"))
        # logger.info(f"all_files count={len(all_files)}")
        # logger.info(glob.glob(os.path.join('data/departure', "departure_documents*.csv")))
        # departure_documents = pd.concat((pd.read_csv(f) for f in all_files), ignore_index=True)
        departure_documents = pd.read_csv("data/departure_documents.csv")
        Loader.create_stations_dim(departure_documents)
        Loader.create_routes_dim(departure_documents)
        Loader.create_customers_dim(departure_documents)
        Loader.create_cargo_types_dim(departure_documents)

    @staticmethod
    def create_stations_dim(departure_documents):
        dim_stations = departure_documents[
            ['DEPARTURESTATIONID', 'DEPARTURESTATIONNAME', 'DEPARTURESTATIONLON', 'DEPARTURESTATIONLAT']]
        dim_stations.rename(
            columns={"DEPARTURESTATIONID": "id", "DEPARTURESTATIONNAME": "name", "DEPARTURESTATIONLON": "lon",
                     "DEPARTURESTATIONLAT": "lat"}, inplace=True)
        # unique_rows = dim_stations.drop_duplicates(['id', 'name', 'lon', 'lat'])[['id', 'name', 'lon', 'lat']]
        unique_rows = dim_stations.drop_duplicates(['id'])[['id', 'name', 'lon', 'lat']]
        Transform.df_to_sql('dim_stations', unique_rows)

    @staticmethod
    def create_routes_dim(departure_documents):
        dim_routes = departure_documents[
            ['ROUTE_ID', 'ROUTENAME', 'DISTANCE', 'STANDARDTURN', 'DEPARTUREDOWNTIMERATE', 'ARRIVALDELAYRATE',
             'WAGONCOUNTLOADING', 'TRAINWEIGHTNET', 'TRAINWEIGHTGROSS', 'DEPARTURESTATIONID', 'DESTINATIONSTATIONID']]
        dim_routes.rename(
            columns={'ROUTE_ID': 'id', 'ROUTENAME': 'name', 'DISTANCE':'distance', 'STANDARDTURN':'standard_turn',
                     'DEPARTUREDOWNTIMERATE':'departure_down_time_rate', 'ARRIVALDELAYRATE':'arrival_delay_rate',
                     'WAGONCOUNTLOADING':'wagon_count_loading', 'TRAINWEIGHTNET':'train_weight_net',
                     'TRAINWEIGHTGROSS':'train_weight_gross', 'DEPARTURESTATIONID':'departure_station_id',
                     'DESTINATIONSTATIONID':'destination_station_id'}, inplace=True)
        unique_rows = dim_routes.drop_duplicates(['id'])[['id', 'name', 'distance', 'standard_turn',
                                                          'departure_down_time_rate', 'arrival_delay_rate',
                                                          'wagon_count_loading',
                                                          'train_weight_net', 'train_weight_gross',
                                                          'departure_station_id',
                                                          'destination_station_id']]
        Transform.df_to_sql('dim_routes', unique_rows)

    @staticmethod
    def create_customers_dim(departure_documents):
        dim_senders = departure_documents[['SENDER_ID', 'SENDERNAME']]
        dim_senders.rename(columns={'SENDER_ID': 'id', 'SENDERNAME': 'name'}, inplace=True)

        dim_receivers = departure_documents[['RECEIVER_ID', 'RECEIVERNAME']]
        dim_receivers.rename(columns={'RECEIVER_ID': 'id', 'RECEIVERNAME': 'name'}, inplace=True)

        dim_customers = pd.concat([dim_senders, dim_receivers], ignore_index=True)
        unique_customers = dim_customers.drop_duplicates(['id'])[['id', 'name']]
        Transform.df_to_sql('dim_customers', unique_customers)

    @staticmethod
    def create_cargo_types_dim(departure_documents):
        dim_cargo_types = departure_documents[['CARGOTYPE_ID', 'CARGOTYPENAME']]
        dim_cargo_types.rename(columns={'CARGOTYPE_ID': 'id', 'CARGOTYPENAME': 'name'}, inplace=True)
        unique_cargo_types = dim_cargo_types.drop_duplicates(['id'])[['id', 'name']]
        Transform.df_to_sql('dim_cargo_types', unique_cargo_types)

    @staticmethod
    def create_facts_departures():
        departure_documents = pd.read_csv("data/departure_documents.csv")
        facts_departures = departure_documents[['DEPARTUREDOCUMENTID', 'ROUTE_ID', 'TRANSPORTATIONSHEETID', 'SENDER_ID',
                                                'RECEIVER_ID', 'WAGONCOUNTLOADING1', 'TRAINWEIGHTGROSS1',
                                                'TRAINWEIGHTNET1', 'CARGOTYPE_ID', 'EMPTYTYPES_ID', 'DOWNTIME', 'AXISCOUNT',
                                                'ENDPENDINGDATE', 'DEPARTUREDATE', 'CALENDARSTAMP']]
        facts_departures.rename(columns={'DEPARTUREDOCUMENTID': 'id', 'ROUTE_ID': 'route_id',
                                         'TRANSPORTATIONSHEETID': 'transportation_sheet_id',
                                         'SENDER_ID': 'sender_id', 'RECEIVER_ID': 'receiver_id',
                                         'WAGONCOUNTLOADING1': 'real_wagon_loading',
                                         'TRAINWEIGHTGROSS1': 'real_train_weight_gross',
                                         'TRAINWEIGHTNET1': 'real_train_wight_net', 'CARGOTYPE_ID': 'cargo_type_id',
                                         'EMPTYTYPES_ID': 'empty_types_id', 'DOWNTIME' : 'down_time',
                                         'AXISCOUNT': 'axis_count', 'ENDPENDINGDATE': 'end_pending_dttm',
                                         'DEPARTUREDATE': 'departure_dttm','CALENDARSTAMP': 'calendar_stamp_dttm'},
                                inplace=True)
        unique_facts_departures = facts_departures.drop_duplicates(['id'])[['id', 'route_id', 'transportation_sheet_id',
                                                                            'sender_id', 'receiver_id',
                                                                            'real_wagon_loading', 'real_train_weight_gross',
                                                                            'real_train_wight_net', 'cargo_type_id',
                                                                            'empty_types_id', 'down_time',
                                                                            'axis_count', 'end_pending_dttm',
                                                                            'departure_dttm', 'calendar_stamp_dttm']]
        Transform.df_to_sql('facts_departures', unique_facts_departures, Transform.departures_sql_file)

    @staticmethod
    def create_facts_giving():
        giving_documents = pd.read_csv("data/giving_documents.csv")
        facts_giving_documents = giving_documents[['GIVINGDOCUMENTID', 'GIVINGDATE', 'ENROLLMENTDATE', 'DOWNTIMETRAIN',
                                                   'DEPARTUREDOCUMENTID', 'TRANSPORTATIONSHEETID', 'INTERVALGIVING',
                                                   'DEPARTUREDATE', 'ENDPENDINGDATE', 'IDLETRAIN', 'TRAVELTIME',
                                                   'CREWDOWNTIME', 'CREWDOWNTIMENOPLAN', 'INTERMEDIATEDOWNTIME']]
        facts_giving_documents.rename(columns={'GIVINGDOCUMENTID': 'id', 'GIVINGDATE': 'giving_dttm',
                                               'ENROLLMENTDATE': 'enrollment_dttm', 'DOWNTIMETRAIN': 'down_time_train',
                                               'DEPARTUREDOCUMENTID': 'facts_departures_id',
                                               'TRANSPORTATIONSHEETID': 'transportation_sheet_id',
                                               'INTERVALGIVING': 'interval_giving', 'DEPARTUREDATE': 'departure_dttm',
                                               'ENDPENDINGDATE': 'end_pending_dttm', 'IDLETRAIN': 'idle_train',
                                               'TRAVELTIME': 'travel_time', 'CREWDOWNTIME': 'crew_down_time',
                                               'CREWDOWNTIMENOPLAN': 'crew_down_time_plan',
                                               'INTERMEDIATEDOWNTIME': 'intermediate_down_time'},
                                      inplace=True)
        unique_facts_giving = facts_giving_documents.drop_duplicates(['id'])[['id', 'giving_dttm', 'enrollment_dttm',
                                                                              'down_time_train', 'facts_departures_id',
                                                                              'transportation_sheet_id', 'interval_giving',
                                                                              'departure_dttm', 'end_pending_dttm',
                                                                              'idle_train', 'travel_time',
                                                                              'crew_down_time', 'crew_down_time_plan',
                                                                              'intermediate_down_time']]
        Transform.df_to_sql('facts_givings', unique_facts_giving, Transform.giving_sql_file)