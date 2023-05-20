from processor import Processor
from sqlite3 import connect
from pandas import read_sql


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
            endpoint = "http://127.0.0.1:9999/blazegraph/sparql"
            query = """
            PREFIX rdf:  <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
            PREFIX schema: <https://schema.org/>

            SELECT ?id
            WHERE {
                ?id 
            }
            """
            df_sparql = get(endpoint, query, True)
            return df_sparql
