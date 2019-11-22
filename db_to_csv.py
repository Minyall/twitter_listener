import json

import dataset
import pandas as pd

def unstringify_nested_dict(df, col):
    if col in df.columns:
        unpacked = df[df[col].notna()][col].apply(lambda x: pd.Series(json.loads(x)))
        unpacked = unpacked.add_prefix(col + '.')
        df.drop(columns=[col], inplace=True)
        df = df.merge(unpacked, left_index=True, right_index=True, how='left')
    return df


def load_as_dataframe(dbname, cols_to_unpack=None):
    if not dbname.endswith('.db'):
        dbname = dbname+'.db'
    db = dataset.connect(f"sqlite:///{dbname}")
    result = db['tweets'].all()
    df = pd.DataFrame(result)
    if cols_to_unpack != None:

        for col in cols_to_unpack:
            df = unstringify_nested_dict(df, col)

    return df


if __name__ == 'main':
    import argparse

    parser = argparse.ArgumentParser(description='A general pupose Twitter Monitor')
    parser.add_argument('-db', '--dbname', action='store', type=str, required=True)
    parser.add_argument('-u', '--unpack', action='store', nargs='*', required=False)
    args = vars(parser.parse_args())

    dbname = args['dbname']
    cols_to_unpack = args['unpack']

    df = load_as_dataframe(dbname=dbname, cols_to_unpack=cols_to_unpack)
    df.to_csv(dbname+".csv", index=True)


