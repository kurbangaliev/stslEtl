import numpy as np

class Transform:
    dims_sql_file = "dims.sql"
    departures_sql_file = "departures.sql"
    giving_sql_file = "giving.sql"

    @staticmethod
    def df_to_sql(table_name, df, filename = dims_sql_file):
        columns = ','.join(df.columns)
        path = f'data/sql/{filename}'

        with open(path, 'a+', encoding='utf8') as fl:
            for _, record in df.iterrows():
                fl.write(f'INSERT INTO {table_name} ({columns}) VALUES (' + Transform.process_values_for_sql(
                    record.values.tolist()) + ');\n')

    @staticmethod
    def process_values_for_sql(values):
        value_list = []

        for v in values:
            if str(v) == 'nan' or v == np.nan:
                value_list.append('NULL')
            elif type(v) == str:
                value_list.append(f"'{v}'")
            else:
                value_list.append(str(v))

        return ','.join(value_list)