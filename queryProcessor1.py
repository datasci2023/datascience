from processorTemp import Processor
from sqlite3 import connect
from pandas import read_sql, read_sql_query


class QueryProcessor(Processor):
    def __init__(self, dbPathOrUrl):
        super().__init__(dbPathOrUrl)

    def getEntityById(self, entityId: str):
        # return data frame for a specific identifier
        # reading data from sqlite or blazegraph
        # for json do we need to wrap the .. for RDF triplestore??
        if self.dbPathOrUrl:  # dummy condition
            with connect(self.dbPathOrUrl) as con:
                query = """
                SELECT * FROM EntitiesWithMetadata
                FULL OUTER JOIN Annotations ON EntitiesWithMetadata.metadata_internal_id = Annotations.annotation_targets
                FULL OUTER JOIN Images ON Annotations.annotation_bodies = Images.images_internal_id
                FULL OUTER JOIN Creators ON EntitiesWithMetadata.creator = Creators.creator_internal_id
                WHERE EntitiesWithMetadata.id = ? OR Annotations.annotation_ids = ? OR Images.image_ids = ?
                """
                cursor = con.cursor()
                cursor.execute(query, (entityId, entityId, entityId))
                df = read_sql_query(query, con, params=(entityId, entityId, entityId))
                # df = read_sql(query, con)
                return df
        # else:
        #     endpoint = "http://127.0.0.1:9999/blazegraph/sparql"
        #     query = """
        #     PREFIX rdf:  <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
        #     PREFIX schema: <https://schema.org/>

        #     SELECT ?id
        #     WHERE {
        #         ?id 
        #     }
        #     """
        #     df_sparql = get(endpoint, query, True)
        #     return df_sparql


print(QueryProcessor.getEntityById(self=QueryProcessor, entityId="https://dl.ficlit.unibo.it/iiif/2/45498/full/699,800/0/default.jpg"))
# it works like that, we still have to figure out how to use properly the variable dbPathOrUrl