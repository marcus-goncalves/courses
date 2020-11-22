import os
import json
from tweepy import OAuthHandler, Stream, StreamListener
from datetime import datetime
from dotenv import load_dotenv

load_dotenv(os.path.join('.', '.env'))

CONSUMER_KEY: str = os.getenv('API_KEY')
CONSUMER_SECRET: str = os.getenv('API_SECRET_KEY')
ACCESS_KEY: str = os.getenv('ACCESS_TOKEN')
ACCESS_SECRET: str = os.getenv('ACCESS_SECRET_TOKEN')

now = datetime.now().strftime('%Y-%m-%d-%H%M%S')
out_file = open(f'collected_tweets_{now}.txt', 'w')

class Collector(StreamListener):
    def on_data(self, data) -> bool:
        # print(data)
        item: str = json.dumps(data)
        out_file.write(item + '\n')
        return True
    
    def on_error(self, status) -> None:
        print(status)

if __name__ == "__main__":
    listener = Collector()
    auth = OAuthHandler(CONSUMER_KEY, CONSUMER_SECRET)

    auth.set_access_token(ACCESS_KEY, ACCESS_SECRET)

    stream = Stream(auth, listener)
    stream.filter(track=['Bolsonaro'])
