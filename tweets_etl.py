import pandas as pd
import sqlite3
import logging

def process_tweets():
    logging.info("Starting data extraction and transformation...")
    try:
        df = pd.read_csv("/home/sahiln/airflow/phone_tweets.csv", on_bad_lines='error', low_memory=False, delimiter=',')
        dataFrame = df.copy()
        dataFrame = dataFrame.dropna()
        dataFrame['tweet'] = dataFrame['tweet'].str.lower().str.strip()
        dataFrame['date'] = pd.to_datetime(dataFrame['date'])
        dataFrame['day']=dataFrame['date'].dt.day_name()
        dataFrame['month']=dataFrame['date'].dt.month
        dataFrame['year']=dataFrame['date'].dt.year
        dataFrame = dataFrame.drop(['id', 'date'], axis=1)
        dataFrame.to_csv("/home/sahiln/airflow/data.csv", index=False)
        logging.info("Data extraction and transformation complete.")
    except Exception as e:
        logging.error(f"Error in twitter_data: {e}")
        raise

def load_tweets_to_db():
    logging.info("Starting to load data into SQLite database...")
    try:
        conn = sqlite3.connect("/home/sahiln/airflow/phonetweet.db")
        c = conn.cursor()
        c.execute("DROP TABLE IF EXISTS phone_tweets")
        conn.commit()
        c.execute(
            """
            CREATE TABLE IF NOT EXISTS phone_tweets (
                phone_name INTEGER NOT NULL,
                user_name TEXT NOT NULL,
                tweet TEXT NOT NULL,
                language TEXT NOT NULL,
                replies_count INTEGER NOT NULL,
                retweets_count INTEGER NOT NULL,
                likes_count INTEGER NOT NULL,
                hashtags TEXT,
                link TEXT NOT NULL,
                day TEXT NOT NULL,
                month INTEGER NOT NULL,
                year INTEGER NOT NULL
            )
            """
        )

        chunk_size = 10000
        for chunk in pd.read_csv("/home/sahiln/airflow/data.csv", chunksize=chunk_size):
            chunk.to_sql('phone_tweets', conn, index=False, if_exists='append') 
        conn.commit()
        conn.close()
        logging.info("Data successfully loaded into database.")
    
    except Exception as e:
        logging.error(f"Error in load_data: {e}")
        raise