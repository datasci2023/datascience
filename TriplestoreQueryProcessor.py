from pandas import DataFrame
from queryProcessor import QueryProcessor

from pandas.io.json import json_normalize
from SPARQLWrapper import SPARQLWrapper, JSON


class TriplestoreQueryProcessor(QueryProcessor):
    def __init__(self, entityId):
        super().__init__(entityId)

    def getAllCanvases(self) -> DataFrame:
        endpoint = self.getDbPathOrUrl()
        query = """
        PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
        PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
        PREFIX p1: <https://github.com/datasci2023/datascience/res/>
        PREFIX p2: <https://github.com/datasci2023/datascience/attr/>
        SELECT ?id ?literal_id ?label
        WHERE {
            ?id p2:id ?literal_id;
                rdf:type p1:Canvas;
                rdfs:label ?label .
        }
        """
        sparql = SPARQLWrapper(endpoint)
        sparql.setQuery(query)
        sparql.setReturnFormat(JSON)
        result = sparql.query().convert()
        return json_normalize(result)  # ???

    def getAllCollections(self) -> DataFrame:
        endpoint = self.getDbPathOrUrl()
        query = """
        PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
        PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
        PREFIX p1: <https://github.com/datasci2023/datascience/res/>
        PREFIX p2: <https://github.com/datasci2023/datascience/attr/>
        SELECT ?id ?literal_id ?label ?items
        WHERE {
            ?id p2:id ?literal_id;
                rdf:type p1:Collection;
                rdfs:label ?label;
                p2:items ?items .
        }
        """
        sparql = SPARQLWrapper(endpoint)
        sparql.setQuery(query)
        sparql.setReturnFormat(JSON)
        result = sparql.query().convert()
        return json_normalize(result["type"]["collection"])  # ???

    def getAllManifests(self) -> DataFrame:
        endpoint = self.getDbPathOrUrl()
        query = """
        PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
        PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
        PREFIX p1: <https://github.com/datasci2023/datascience/res/>
        PREFIX p2: <https://github.com/datasci2023/datascience/attr/>
        SELECT ?id ?literal_id ?label ?items
        WHERE {
            ?id p2:id ?literal_id;
                rdf:type p1:Manifest;
                rdfs:label ?label;
                p2:items ?items .
        }
        """
        sparql = SPARQLWrapper(endpoint)
        sparql.setQuery(query)
        sparql.setReturnFormat(JSON)
        result = sparql.query().convert()
        return json_normalize(result["type"]["manifest"])  # ???

    def getCanvasesInCollection(self, collectionId: str) -> DataFrame:
        endpoint = self.getDbPathOrUrl()
        query = (
            """
        PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
        PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
        PREFIX p1: <https://github.com/datasci2023/datascience/res/>
        PREFIX p2: <https://github.com/datasci2023/datascience/attr/>
        SELECT ?collection_id ?manifest_id ?canvas_id ?literal_collection_id ?literal_canvas_id ?label 
        WHERE {
            ?literal_collection_id a \""""
            + collectionId
            + """\" .
            ?collection_id p2:id ?literal_collection_id;
                rdf:type Collection  #####can make this faster
                p2:items ?manifest_id .
            ?manifest_id p2:items ?canvas_id .
            ?canvas_id p2:id ?literal_canvas_id;
                rdf:type p1:Canvas;
                rdfs:label ?label .                
        }
        """
        )
        sparql = SPARQLWrapper(endpoint)
        sparql.setQuery(query)
        sparql.setReturnFormat(JSON)
        result = sparql.query().convert()
        return json_normalize(result)  # ???

    def getCanvasesInManifest(self, query, endpoint, manifestId: str) -> DataFrame:
        endpoint = self.getDbPathOrUrl()
        query = (
            """
        PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
        PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
        PREFIX p1: <https://github.com/datasci2023/datascience/res/>
        PREFIX p2: <https://github.com/datasci2023/datascience/attr/>
        SELECT ?manifest_id ?canvas_id ?literal_manifest_id ?literal_canvas_id ?label 
        WHERE {
            ?literal_manifest_id a \""""
            + manifestId
            + """\" .
            ?manifest_id p2:id ?literal_manifest_id;
                p2:items ?canvas_id .
            ?canvas_id p2:id ?literal_canvas_id;
                rdf:type p1:Canvas;
                rdfs:label ?label .                
        }
        """
        )
        sparql = SPARQLWrapper(endpoint)
        sparql.setQuery(query)
        sparql.setReturnFormat(JSON)
        result = sparql.query().convert()
        return json_normalize(result)  # ???

    def getEntitiesWithLabel(self, label: str) -> DataFrame:
        endpoint = self.getDbPathOrUrl()
        query = (
            """
        PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
        PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
        PREFIX p1: <https://github.com/datasci2023/datascience/res/>
        PREFIX p2: <https://github.com/datasci2023/datascience/attr/>
        SELECT ?id ?items ?type
        WHERE {
            ?label a \""""
            + label
            + """\" .
            ?id rdfs:label ?label;
                rdf:type ?type .
            OPTIONAL { ?id p2:items ?items}
        }
        """
        )
        # query = (
        #     """
        # PREFIX rdf ???
        # PREFIX rdfs
        # PREFIX p1: <https://github.com/datasci2023/datascience/res/>
        # PREFIX p2: <https://github.com/datasci2023/datascience/attr/>
        # SELECT ?collection_id ?manifest_id ?canvas_id ?literal_id ?label
        # WHERE {
        #     ?collection_id rdfs:label \""""
        #     + label
        #     + """\" ;
        #         p2:id ?literal_collection_id;
        #         rdf:type p1:Collection;
        #         p2:items ?manifest_id .
        # }
        # """
        # )
        sparql = SPARQLWrapper(endpoint)
        sparql.setQuery(query)
        sparql.setReturnFormat(JSON)
        result = sparql.query().convert()
        return json_normalize(result)  # ???

    def getManifestsInCollection(self, collectionId: str) -> DataFrame:
        endpoint = self.getDbPathOrUrl()
        query = (
            """
        PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
        PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
        PREFIX p1: <https://github.com/datasci2023/datascience/res/>
        PREFIX p2: <https://github.com/datasci2023/datascience/attr/>
        SELECT ?collection_id ?manifest_id ?canvas_id ?literal_collection_id ?literal_manifest_id ?label 
        WHERE {
            ?literal_collection_id a \""""
            + collectionId
            + """\" .
            ?collection_id p2:id ?literal_collection_id;
                p2:items ?manifest_id .
            ?manifest_id p2:id ?literal_manifest_id;
                rdf:type p1:Manifest;
                rdfs:label ?label;
                p2:items ?canvas_id.                
        }
        """
        )
        sparql = SPARQLWrapper(endpoint)
        sparql.setQuery(query)
        sparql.setReturnFormat(JSON)
        result = sparql.query().convert()
        return json_normalize(result)  # ???
