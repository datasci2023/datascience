import os
from processor import Processor
from sqlite3 import connect
from pandas import read_sql, read_sql_query
from sparql_dataframe import get


class QueryProcessor(Processor):
    def __init__(self):
        super().__init__()

    def getEntityById(self, entityId: str):
        # return data frame for a specific identifier
        # reading data from sqlite or blazegraph
        # for json do we need to wrap the .. for RDF triplestore??

        path = self.getDbPathOrUrl()

        if os.path.isfile(path) or path.endswith(".db"):
            with connect(self.dbPathOrUrl) as con:
                query = f"""
                SELECT * FROM EntitiesWithMetadata
                LEFT JOIN Annotations ON EntitiesWithMetadata.metadata_internal_id == Annotations.annotation_targets
                LEFT JOIN Images ON Annotations.annotation_bodies == Images.images_internal_id
                LEFT JOIN Creators ON EntitiesWithMetadata.creator == Creators.creator_internal_id
                WHERE EntitiesWithMetadata.id = '{entityId}' OR Annotations.annotation_ids = '{entityId}' OR Images.image_ids = '{entityId}'
                """
                df = read_sql(query, con)
                return df
        elif path.startswith("https:") or path.startswith("http:"):
            endpoint = self.getDbPathOrUrl()
            query = (
                (
                    """
            PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
            PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
            PREFIX pomodoro: <https://github.com/datasci2023/datascience/class/>
            PREFIX feslegen: <https://github.com/datasci2023/datascience/attribute/>
            PREFIX spaghetti:  <https://github.com/datasci2023/datascience/relation/>
        
            SELECT ?id ?type ?label 
            WHERE {
                ?id rdf:type ?type ;
                    rdfs:label ?label .
                FILTER (?id = <%s>)
            }
            """
                )
                % entityId
            )
            df_sparql = get(endpoint, query, True)
            return df_sparql
        else:
            print("Error!!!")

            df_sparql = get(endpoint, query, True)
            return df_sparql
