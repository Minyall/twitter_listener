# Basic Twitter Listener


## CLI Scripts
### `live_listener.py`

Main file to collect tweets from the livestream.

##### Command Line Arguments

- `--query` Your query used to filter out relevant tweets from the live stream. You can specify multiple queries seperated by a space.
- `--dbname` The name to use for your collection of tweets. Allows you to specify a different name for each project to keep your data seperate.
- `--retweets` Use if you would like to collect retweets. Default behaviour is to ignore retweets.


```angular2
Example
python live_listener.py --query kittens dogs #animals --dbname animal_tweets --retweets
```

### `db_to_csv.py`

Allows you to dump the data from the database to a CSV file.

##### Command Line Arguments

- `--dbname` Name of the database you want to export.
- `cols_to_unpack` the columns of nested data that want to unpack. Specify multiple names seperated by spaces (*see note below).

```angular2
Example
python db_to_csv.py --dbname animal_tweets --unpack retweeted_status user 
```

##### * Note on Unpacking

The listener collects ALL the fields available from Twitter. However some fields include nested data, such as the `user` field or the `retweeted_status` field.
To unpack these at the point of building your CSV specify the columns in the `--unpack` argument.

## Importable Functions

If you would prefer you can import functions from `functions.py` for extracting data from the database and unpacking.

### `load_as_dataframe`
Returns a Pandas DataFrame
```angular2
from functions import load_as_dataframe

df = load_as_dataframe(dbname='animal_tweets', cols_to_unpack=['retweeted_status','user'])

```

### `unstringify_nested_dict`
Unpack an individual column in an already loaded DataFrame. Returns the original datatframe with the nested fields as their own columns prefixed with the original column name.

```angular2
from functions import unstringify_nested_dict

df = unstringify_nested_dict(df, col='retweeted_status')

```

### Requirements
A Requirements.txt is included. However the script should work fine if you have `tweepy`, `pandas` and `dataset`

```angular2
pip install pandas
pip install tweepy
pip install datatset
```