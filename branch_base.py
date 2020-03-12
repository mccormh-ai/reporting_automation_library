import pandas as pd
from dataframe_lib.datetime_operations import convertDateColumns


class Branch():
    def __init__(self, source_dataframe, primary_key, date_column=None):
        self.dataframe = source_dataframe
        self.pk = primary_key,
        if date_column is not None:
            self.dataframe[date_column] = pd.to_datetime(self.dataframe[date_column])
        else:
            self.dataframe = convertDateColumns(self.dataframe)

    def column_manipulation(self, base_dimension_df, rename_dict):
        # deal with rename_columns
        #
