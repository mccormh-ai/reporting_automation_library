import pandas as pd

class Joins():
    def __init__(self, source_df, dimension_df, pk):
        self.df1 = source_df
        self.df2 = dimension_df
        self.pk = str(pk)

    def merge_updateShared_appendNew(self):
        d1 = self.df1.set_index(self.pk)
        d2 = self.df2.set_index(self.pk)

        result_df = df1.combine_first(df2)\
                    .reset_index()\
                    .reindex(columns=df1.columns))
        return result_df

    def _explicit_updateShared_appendNew(self, test=True):
        merge_df = pd.merge(self.df1, self.df2, indicator='i', on=self.pk
                            , how='outer', suffixes=('', '_y'))
        left_df = merge_df.query('i == "left_only" or i == "both"')\
            [list(loaded_df.columns)]
        both_count = len(merge_df.query('i == "both"').index)
        y_columns = [x + '_y' for x in list(self.df2.columns)]
        y_columns.remove(f"{pk}_y")
        y_columns.insert(pk_position, pk)
        right_df = merge_df.query('i == "right_only"')[y_columns]
        right_df.columns = list(self.df1.columns)
        concat_df = pd.concat([left_df, right_df], axis=0)
        # Keep Nulls across PK
        dedup_df = concat_df.sort_values(date_column).dropna(subset=[pk]).drop_duplicates(
            subset=dedup_column, keep='last')
        null_df = concat_df[concat_df[pk] == 0]
        new_master_df = pd.concat([null_df, dedup_df], axis=0)


        if test:
            print(f"Master DF consisted of {len(self.df2.index)} rows.")
            print(f"New Master has {len(new_master_df.index)} rows.")
            print(f"Giving {len(new_master_df.index) - len(self.df2.index)} new rows to be added")
            print(f"and {both_count} rows to be updated.")
            if len(new_master_df.index) < len(self.df2.index):
                print("Calculation Funky! Look at logic!")
                raise ValueError
        return new_master_df
