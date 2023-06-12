from processor import Processor
from sqlite3 import connect
from pandas import read_sql

from sparql_dataframe import get


class QueryProcessor(Processor):
    def __init__(self):
        super().__init__()

    def getEntityById(self, entityId: str):
        # return data frame for a specific identifier
        # reading data from sqlite or blazegraph
        # for json do we need to wrap the .. for RDF triplestore??

        if self.dbPathOrUrl:  # dummy condition
            with connect(self.dbPathOrUrl) as con:
                query = """
                SELECT * FROM 'Canvas' WHERE id == '{entityId}'
                UNION ALL
                SELECT * FROM 'Manifest' WHERE id == '{entityId}'
                """
            df = read_sql(query, con)
            return df
        else:
            endpoint = self.getDbPathOrUrl()
            query = (
                """
            PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
            PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
            PREFIX p1: <https://github.com/datasci2023/datascience/res/>
            PREFIX p2: <https://github.com/datasci2023/datascience/attr/>
            SELECT ?id ?items ?type ?label 
            WHERE {
                ?id a '"""
                + str(entityId)
                + """' .
                ?id rdf:type ?type ;
                    rdfs:label ?label .
                OPTIONAL { ?id p2:items ?items}
            }
            """
            )

            df_sparql = get(endpoint, query, True)
            return df_sparql
