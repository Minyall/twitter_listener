# Basic Twitter Listener


## CLI Scripts
### `live_listener.py`

Main file to collect tweets from the livestream.

##### Command Line Arguments

- `--query` Your query used to filter out relevant tweets from the live stream. You can specify multiple queries seperated by a space.
- `--dbname` The name to use for your collection of tweets. Allows you to specify a different name for each project to keep your data seperate.
- `--retweets` Toggle to switch on the collection of retweets. Default behaviour is to ignore retweets.
- `--verbosity` Sets how often the log will update in seconds. Controls both the logging to disk and the output 
in the terminal Default: 60
- `--show_sample` Toggle to switch on the display of tweet text in logs and the terminal display. By default this is switched off.
- `--sample_n` Sets the number of tweets to display as a sample. Default: 5
- `--sample_len` Character limit of displayed samples  - truncates text at the character limit. Good for keeping a clean display. Default: 100


```angular2
Example

# You will need to escape hashtags using \ for them to be accepted by the shell command.
python live_listener.py --query kittens dogs \#animals --dbname animal_tweets --retweets
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

If you would prefer you can import functions from `db_to_csv.py` for extracting data from the database and unpacking.

### `load_as_dataframe`
Returns a Pandas DataFrame
```angular2
from db_to_csv import load_as_dataframe

df = load_as_dataframe(dbname='animal_tweets', cols_to_unpack=['retweeted_status','user'])

```

### `unstringify_nested_dict`
Unpack an individual column in an already loaded DataFrame. Returns the original dataframe with the nested fields as their own columns prefixed with the original column name.

```angular2
from db_to_csv import unstringify_nested_dict

df = unstringify_nested_dict(df, col='retweeted_status')

```

### Requirements
A Requirements.txt is included. However the script should work fine if you have `tweepy`, `pandas` and `dataset`

```angular2
pip install pandas
pip install tweepy
pip install dataset
```