from clickhouse_sqlalchemy import make_session
from sqlalchemy import create_engine

default_database = "default"

class ClickhouseConnector():

    def connect(self, host, port, user="root",password="", database=default_database):
        self._uri = "clickhouse+http://{}:{}@{}:{}/{}".format(user,password,host,port,database)
        self._engine = create_engine(self._uri)
        self._session = None

    def query_with_session(self,statement):
        if self._session is None:
            self._session = make_session(self._engine)
        return self._session.execute(statement)

    def reset_session(self):
        if self._session is not None:
            self._session.close()
            self._session = None

    def fetch_all(self, statement):
        cursor = self.query_with_session(statement)
        data_list = list()
        for item in cursor.fetchall():
            data_list.append(list(item))
        cursor.close()
        return data_list

if __name__ == '__main__':
    from config import clickhouse_config
    connector = ClickhouseConnector()
    connector.connect(**clickhouse_config)
    print(connector.fetch_all("show databases"))
    print(connector.fetch_all("select * from t1"))