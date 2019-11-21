import pandas as pd
import dataset
import json

def unstringify_nested_dict(df, col):
    if col in df.columns:
        unpacked = df[df[col].notna()][col].apply(lambda x: pd.Series(json.loads(x)))
        unpacked = unpacked.add_prefix(col + '.')
        df.drop(columns=[col], inplace=True)
        df = df.merge(unpacked, left_index=True, right_index=True, how='left')
    return df

def load_as_dataframe(dbname, cols_to_unpack=None):
    db = dataset.connect(f"sqlite:///{dbname}.db")
    result = db['tweets'].all()
    df = pd.DataFrame(result)
    if cols_to_unpack != None:

        for col in cols_to_unpack:
            df = unstringify_nested_dict(df, col)

    return df

def chunks(l, n):
    """Yield successive n-sized chunks from l."""
    for i in range(0, len(l), n):
        yield l[i:i + n]