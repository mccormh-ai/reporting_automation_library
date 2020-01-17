import pandas as pd

class Joins():
    def __init__(self, source_df, dimension_df, pk):
        self.df1 = source_df
        self.df2 = dimension_df
        self.pk = str(pk)

    def merge_update_shared_append_new(self):
        d1 = self.df2.set_index(self.pk)
        d2 = self.df1.set_index(self.pk)

        result_df = df1.combine_first(df2)\
                    .reset_index()\
                    .reindex(columns=df1.columns))
        return result_df

    def print_results(self):
        pass
