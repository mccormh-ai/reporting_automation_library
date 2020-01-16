import pandas as pd
import os
import datetime

class Platform_Obj():
    def __init__(self, table, engine, session, file_path, _date=datetime.datetime.today()):
        self.target_table = table
        self.engine = engine
        self.session = session
        self.file_path = file_path
        self._date = _date
        self.master_df = self.get_tbl_as_df(table_name=self.target_table.__tablename__)

    @staticmethod
    def rename_columns(params, dataframe):
        # Can only process renaming columns at the moment
        return dataframe.rename(columns=params)

    @staticmethod
    def declare_types(params, dataframe):
        valid_type_changes = [
            "int",
            "str",
            "datetime",
            "float"
        ]
        if not all(_type in valid_type_changes for _type in list(params.values())):
            print("Invalid type in params")
            print(f"Types in params: {list(params.values())}")
            raise ValueError
        if not all(_column in list(dataframe.columns) for _column in list(params.keys())):
            print("Key does not exist in df columns...")
            print(f"Columns in DF: {list(dataframe.columns)}")
            print(f"Columns designated in params: {list(params.keys())}")

        df = dataframe.copy()
        for column_header, _type in params.items():
            try:
                if _type == "int":
                    df[column_header] = df[column_header].astype(int)
                elif _type == "str":
                    df[column_header] = df[column_header].astype(str)
                elif _type == "datetime":
                    df[column_header] = pd.to_datetime(df[column_header])
                elif _type == "float":
                    try:
                        df[column_header] = df[column_header].str.replace(',', '')
                    except Exception as e:
                        print(e)
                    df[column_header] = df[column_header].astype(float)
            except KeyError as e:
                print(list(df.columns))
                print(e)
                print(list(set(params.keys()) - set(df.columns)))
                raise KeyError
        return df

    def get_tbl_as_df(self, table_name):
        try:
            session = self.session()
            # qry = session.query(table)
            df = pd.read_sql(f"Select * from {table_name}",
                             session.bind
                             )
            session.close()
        except Exception as e:
            print(e)
            df = pd.DataFrame()
        return df

    def sort_align_to_master(self, loaded_df):
        if self.master_df.empty:
            return loaded_df
        # print(loaded_df.head())
        # print(self.master_df.head())
        return loaded_df[list(self.master_df.columns)]

    def return_unique_against_master(self, loaded_df, dedup_column, pk, date_column, pk_position=0, test=True):
        # Take the loaded df, compare with the loaded master, return unique rows df
        if self.master_df.empty:
            unique_rows_df = loaded_df.sort_values(date_column).drop_duplicates(subset=dedup_column, keep='last')
        else:
            # Will add column to all incoming data
            # if 'update_time' not in loaded_df.columns:
            #     loaded_df['update_time'] = self._date
            # # Will only be written to once on implimentation
            # if 'update_time' not in self.master_df.columns:
            #     self.master_df['update_time'] = self._date
            for column in dedup_column:
                try:
                    loaded_df[column] = loaded_df[column].fillna(0).astype(int)
                    self.master_df[column] = self.master_df[column].fillna(0).astype(int)
                    print("int")
                except Exception as e:
                    print(e)
                    loaded_df[column] = loaded_df[column].astype(str).str.strip()
                    self.master_df[column] = self.master_df[column].astype(str).str.strip()
            merge_df = pd.merge(loaded_df, self.master_df, indicator='i', on=pk, how='outer', suffixes=('', '_y'))
            left_df = merge_df.query('i == "left_only" or i == "both"')[list(loaded_df.columns)]
            both_count = len(merge_df.query('i == "both"').index)
            y_columns = [x + '_y' for x in list(self.master_df.columns)]
            y_columns.remove(f"{pk}_y")
            y_columns.insert(pk_position, pk)
            right_df = merge_df.query('i == "right_only"')[y_columns]
            right_df.columns = list(loaded_df.columns)

            concat_df = pd.concat([left_df, right_df], axis=0)
            print(concat_df.head().to_string())
            concat_df[date_column] = pd.to_datetime(concat_df[date_column])
            dedup_df = concat_df.sort_values(date_column).dropna(subset=[pk]).drop_duplicates(
                subset=dedup_column, keep='last')
            null_df = concat_df[concat_df[pk] == 0]
            new_master_df = pd.concat([null_df, dedup_df], axis=0)
            # raise ValueError

        if test:
            print(f"Master DF consisted of {len(self.master_df.index)} rows.")
            print(f"New Master has {len(new_master_df.index)} rows.")
            print(f"Giving {len(new_master_df.index) - len(self.master_df.index)} new rows to be added")
            print(f"and {both_count} rows to be updated.")
            if len(new_master_df.index) < len(self.master_df.index):
                print("Calculation Funky! Look at logic!")
                # raise ValueError
        # raise ValueError
        return new_master_df

    def insert_into_table(self, unique_dataframe, insert_type='replace'):
        # Insert into table raw data, process additional columns in post
        # Only except with CPU-intensive tasks
        print(f"Now importing unique values in {self.file_path} into {self.target_table.__tablename__}")
        unique_dataframe.to_sql(name=self.target_table.__tablename__,
                                con=self.engine,
                                if_exists=insert_type,
                                index=False)
        print("Upload Successful")
        print("\n")


    def load_df_from_csv(self, load_all_in_directory=False, rename_param=None, type_params=None, skip_rows=0):
        if load_all_in_directory:
            list_to_concat = [f for f in os.listdir(self.file_path) if f.endswith('.csv')]
            if len(list_to_concat) == 0:
                print(f'There are is no files in {self.file_path} folder. Can not continue...')
                raise ValueError
            list_of_dfs = [pd.read_csv(os.path.join(self.file_path, x), skiprows=skip_rows) for x in list_to_concat]
            df = pd.concat(list_of_dfs, axis=0, ignore_index=True)
        else:
            df = pd.read_csv(self.file_path, skiprows=skip_rows)
        # Type Params is a dict of column header and desired type used prior to write
        # ex: {BillOfLadingID: "int"}
        # Where data type is string of desired type, TODO create more elegant solution for params
        if type_params is not None:
            df = self.declare_types(params=type_params, dataframe=df)
        if rename_param is not None:
            df = self.rename_columns(params=rename_param, dataframe=df)
        df.rename(columns=lambda x: x.replace(" ", "_"), inplace=True)
        return df

    def load_df_from_excel(self, load_all_in_directory=False, sheet_name=0, rename_param=None, type_params=None, skip_rows=0):
        try:
            if load_all_in_directory:
                list_to_concat = [f for f in os.listdir(self.file_path) if f.endswith('.xls')]
                if len(list_to_concat) == 0:
                    print(f'There are is no files in {self.file_path} folder. Can not continue...')
                    raise ValueError
                print("CONCATING EXCEL DFS")
                list_of_dfs = [pd.read_excel(os.path.join(self.file_path, x), sheet_name=sheet_name,
                                             skiprows=int(skip_rows)) for x in list_to_concat]
                df = pd.concat(list_of_dfs, axis=0, ignore_index=True)
            else:
                df = pd.read_excel(self.file_path, sheet_name=sheet_name, skiprows=int(skip_rows))
        except:
            print(self.file_path)
            raise ValueError
        print(df.head())
        if type_params is not None:
            df = self.declare_types(params=type_params, dataframe=df)
        if rename_param is not None:
            df = self.rename_columns(params=rename_param, dataframe=df)
        df.rename(columns=lambda x: x.replace(" ", "_"), inplace=True)
        return df

    # def load_from_shared_drive(self):
