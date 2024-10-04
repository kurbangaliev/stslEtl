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
        logger.info(f"get departure documents count {len(departure_documents)}")
        df = pd.json_normalize(departure_documents)
        df.to_csv(f"data/departure/departure_documents_{thread_num}.csv", index=False)

    @staticmethod
    def get_departure_documents():
        logger.info("get_departure_documents")
        pathlib.Path('data/departure/').mkdir(parents=True, exist_ok=True)

        begin_date = datetime.datetime(2021, 9, 1)
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
        logger.info(f"get departure documents count {len(giving_documents)}")
        df = pd.json_normalize(giving_documents)
        df.to_csv(f"data/giving/giving_documents_{thread_num}.csv", index=False)

    @staticmethod
    def get_giving_documents():
        logger.info("get_giving_documents")
        pathlib.Path('data/giving/').mkdir(parents=True, exist_ok=True)

        begin_date = datetime.datetime(2021, 9, 1)
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
    def create_dims():
        pathlib.Path('data/sql/').mkdir(parents=True, exist_ok=True)
        all_files = glob.glob(os.path.join('data/departure', "departure_documents*.csv"))
        logger.info(f"all_files count={len(all_files)}")
        logger.info(glob.glob(os.path.join('data/departure', "departure_documents*.csv")))
        for file in all_files:
            df = pd.read_csv(file)
            dim_stations = df[['DEPARTURESTATIONID','DEPARTURESTATIONNAME', 'DEPARTURESTATIONLON', 'DEPARTURESTATIONLAT']]
            dim_stations.rename(columns={"DEPARTURESTATIONID": "id", "DEPARTURESTATIONNAME": "name", "DEPARTURESTATIONLON":"lon", "DEPARTURESTATIONLAT":"lat"}, inplace=True)
            Transform.df_to_sql('dim_stations', dim_stations)
        # departure_documents = pd.concat((pd.read_csv(f) for f in all_files), ignore_index=True)
        # dim_stations = departure_documents[['DEPARTURESTATIONID','DEPARTURESTATIONNAME', 'DEPARTURESTATIONLON', 'DEPARTURESTATIONLAT']]
        # dim_stations.rename(columns={"DEPARTURESTATIONID": "id", "DEPARTURESTATIONNAME": "name", "DEPARTURESTATIONLON":"lon", "DEPARTURESTATIONLAT":"lat"}, inplace=True)
        # Transform.df_to_sql('dim_stations', dim_stations)