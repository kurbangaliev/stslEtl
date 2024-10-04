import numpy as np

class Transform:
    @staticmethod
    def df_to_sql(table_name, df):
        columns = ','.join(df.columns)

        if 'dim' in table_name:
            path = 'data/sql/dims.sql'
        else:
            path = 'data/sql/facts.sql'

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