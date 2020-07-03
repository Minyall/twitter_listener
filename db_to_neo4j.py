from neo4j import GraphDatabase
from functions import chunks, unwind_retweets
import pandas as pd
from functions import load_as_dataframe, unpack_column
import argparse
from urllib.parse import urlparse

parser = argparse.ArgumentParser(description='A general pupose Twitter Monitor')
parser.add_argument('-db', '--dbname', action='store', type=str, required=True)
parser.add_argument('-u', '--unpack', action='store', nargs='*', required=False)
args = vars(parser.parse_args())

dbname = args['dbname']
cols_to_unpack = args['unpack']

df = load_as_dataframe(dbname)
df = unpack_column(df, 'retweeted_status')
df = unpack_column(df, 'user')
df = unpack_column(df, 'entities')
df = unwind_retweets(df)

entities_filter = df.dropna(subset=['entities.urls'])['entities.urls'].apply(lambda x: len(x) > 0)
urls_df = df.dropna(subset=['entities.urls'])[entities_filter][['tweet_id', 'entities.urls']].copy()
urls_df = urls_df.explode('entities.urls')
urls_df['url'] = urls_df['entities.urls'].apply(lambda x: x['expanded_url'])
urls_df['netloc'] = urls_df['url'].apply(lambda x: urlparse(x).netloc)
urls_df = urls_df[urls_df['netloc'] != 'twitter.com']
urls_df = urls_df.drop(columns=['entities.urls'])
urls_nodes = urls_df[['netloc']].to_dict(orient='records')
urls_edges = urls_df.to_dict(orient='records')


tweets_cols = ['created_at','favorite_count','retweet_count','tweet_id','text','user.id']
tweets = df[tweets_cols].copy()
tweets = tweets.drop_duplicates(subset=['tweet_id'])
tweets.rename(columns={'user.id':'user_id'}, inplace=True)

user_cols = ['id','screen_name',
              'description',
              'followers_count',
              'friends_count',
              'statuses_count',
              'created_at']
remap = list(map(lambda x: 'user.'+x, user_cols))

users = df[remap].copy()
users.rename(columns=dict(zip(remap,user_cols)), inplace=True)
users.rename(columns={'id':'user_id'}, inplace=True)
users = users.drop_duplicates('user_id')
users = users.dropna(subset=['user_id'])
user_nodes = users.to_dict(orient='records')
tweet_nodes = tweets.to_dict(orient='records')

tweet_user_cols = ['user.id','tweet_id','retweeted_status.id']
tweet_user = df[tweet_user_cols]
author_edges = tweet_user[tweet_user['retweeted_status.id'].isna()].copy()
author_edges.rename(columns={'user.id':'user_id'}, inplace=True)
author_edges = author_edges.drop(columns=['retweeted_status.id'])
author_edges = author_edges.to_dict(orient='records')

rt_edges = tweet_user[~tweet_user['retweeted_status.id'].isna()].copy()
rt_edges = rt_edges.drop(columns=['tweet_id'])
rt_edges.rename(columns={'user.id':'user_id', 'retweeted_status.id':'tweet_id'}, inplace=True)
rt_edges = rt_edges.to_dict(orient='records')


def write_nodes(tx, batch, category, id_val):
    query = f"UNWIND $batch as row MERGE (n:{category.upper()} {{{id_val}:row.{id_val}}}) SET n += row RETURN n"
    tx.run(query,batch=batch)

def write_edges(tx, batch, source_category, source_val, edge_category, target_category, target_val):
    query = f"UNWIND $batch as row MATCH (s:{source_category.upper()} {{{source_val}:row.{source_val}}}), (t:{target_category.upper()} {{{target_val}:row.{target_val}}}) MERGE (s)-[e:{edge_category.upper()}]->(t) RETURN s,e,t"
    tx.run(query, batch=batch)

ndb = GraphDatabase.driver("bolt://localhost:7687", auth=("neo4j", "password"))

def create_constraint(tx, category, id_val):
    query = f"CREATE CONSTRAINT ON (n:{category.upper()}) ASSERT n.{id_val} IS UNIQUE"
    tx.run(query)

def set_constraint(session, category=None, id_val=None):
    res = session.run("CALL db.indexes()")
    constraints = pd.DataFrame(res, columns=res.keys()
                               ).explode('labelsOrTypes'
                                         ).labelsOrTypes.unique().tolist()

    if category not in constraints:
        session.write_transaction(create_constraint, category=category, id_val=id_val)
    return

with ndb.session() as session:
    set_constraint(session, category='USER', id_val='user_id')
    set_constraint(session, category='TWEET',id_val='tweet_id')
    for batch in chunks(user_nodes, 1000):
        session.write_transaction(write_nodes, batch=batch, category='USER', id_val='user_id')
    for batch in chunks(tweet_nodes, 1000):
        session.write_transaction(write_nodes, batch=batch, category='TWEET', id_val='tweet_id')
    for batch in chunks(urls_nodes, 1000):
        session.write_transaction(write_nodes, batch=batch, category='URL', id_val='netloc')
    for batch in chunks(author_edges, 1000):
        session.write_transaction(write_edges,
                                  batch=batch,
                                  source_category='USER',
                                  source_val='user_id',
                                  edge_category='AUTHORED',
                                  target_category='TWEET',
                                  target_val='tweet_id')
    for batch in chunks(rt_edges, 1000):
        session.write_transaction(write_edges,
                                  batch=batch,
                                  source_category='USER',
                                  source_val='user_id',
                                  edge_category='RETWEETED',
                                  target_category='TWEET',
                                  target_val='tweet_id')
    for batch in chunks(urls_edges, 1000):
        session.write_transaction(write_edges,
                                  batch=batch,
                                  source_category='TWEET',
                                  source_val='tweet_id',
                                  edge_category='LINKED_TO',
                                  target_category='URL',
                                  target_val='netloc')


print()

