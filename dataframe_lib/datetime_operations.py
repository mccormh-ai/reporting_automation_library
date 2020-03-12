import pandas as pd
from dateutil import parser

def convertDateColumns(df):
    """ Parse through each column and convert to datetime if possible
        check results to ensure integers are not converted
    """
    object_cols = df.columns.values[df.dtypes.values == 'object']
    date_cols = [c for c in object_cols if testIfColumnIsDate(df[c], num_tries=3)]

    for col in date_cols:
        try:
            df[col] = pd.to_datetime(df[col], coerce=True, infer_datetime_format=True)
        except ValueError:
            pass
    return df


def testIfColumnIsDate(series, num_tries=4):
    """ Test if a column contains date values.
        This can try a few times for the scenerio where a date column may have
        a couple of null or missing values but we still want to parse when
        possible (and convert those null/missing to NaD values)
    """
    if series.dtype != 'object':
        return False

    vals = set()
    for val in series:
        vals.add(val)
        if len(vals) > num_tries:
            break

    for val in list(vals):
        try:
            if type(val) is int:
                continue

            parser.parse(val)
            return True
        except ValueError:
            pass

    return False
