import certifi
from pymongo import MongoClient

from config import c


class Client:
    def __init__(self):
        self.client = self.set_client()

    def set_client(self):
        return MongoClient(c.get("mongodb").get("uri"), tlsCAFile=certifi.where())

    def get_database(self, database):
        return self.client[database]


client = Client()
