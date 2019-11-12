
if __name__ == 'main':
    import argparse
    from functions import load_as_dataframe

    parser = argparse.ArgumentParser(description='A general pupose Twitter Monitor')
    parser.add_argument('-db', '--dbname', action='store', type=str, required=True)
    parser.add_argument('-u', '--unpack', action='store', nargs='*', required=False)
    args = vars(parser.parse_args())

    dbname = args['dbname']
    cols_to_unpack = args['unpack']

    df = load_as_dataframe(dbname=dbname, cols_to_unpack=cols_to_unpack)
    df.to_csv(dbname+".csv", index=True)










