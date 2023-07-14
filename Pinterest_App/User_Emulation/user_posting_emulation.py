import requests
from time import sleep
import random
import sqlalchemy
from sqlalchemy import text
import yaml


with open('config/aws_creds.yaml','r') as f:
    aws_creds = yaml.safe_load(f)

random.seed(100)

class AWSDBConnector:

    def __init__(self):

        self.HOST = aws_creds['HOST']
        self.USER = aws_creds['USER']
        self.PASSWORD = aws_creds['PASSWORD']
        self.DATABASE = aws_creds['DATABASE']
        self.PORT = aws_creds['PORT']
        
    def create_db_connector(self):
        engine = sqlalchemy.create_engine(f"mysql+pymysql://{self.USER}:{self.PASSWORD}@{self.HOST}:{self.PORT}/{self.DATABASE}?charset=utf8mb4")
        return engine


new_connector = AWSDBConnector()


def run_infinite_post_data_loop():
    while True:
        sleep(random.randrange(0, 2))
        random_row = random.randint(0, 11000)
        engine = new_connector.create_db_connector()
        with engine.connect() as connection:
            selected_row = connection.execute(text(f"SELECT * FROM pinterest_data LIMIT {random_row}, 1"))
            for row in selected_row:
                result = dict(row._mapping)
                requests.post("http://localhost:8000/pin/", json=result)
                print(result)


if __name__ == "__main__":
    run_infinite_post_data_loop()
    print('Working')
    
    


