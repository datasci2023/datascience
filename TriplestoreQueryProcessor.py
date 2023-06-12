from pandas import DataFrame
from queryProcessor import QueryProcessor
from sparql_dataframe import get
from rdflib import Graph, Literal, RDF, RDFS, URIRef


class TriplestoreQueryProcessor(QueryProcessor):
    def __init__(self):
        super().__init__()

    def getAllCanvases(self) -> DataFrame:
        endpoint = self.getDbPathOrUrl()
        query = """
        PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
        PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
        PREFIX p1: <https://github.com/datasci2023/datascience/res/>
        SELECT ?id ?label
        WHERE {
            ?id rdf:type p1:Canvas;
                rdfs:label ?label .
        }
        """
        df_sparql = get(endpoint, query, True)
        return df_sparql

    def getAllCollections(self) -> DataFrame:
        endpoint = self.getDbPathOrUrl()
        query = """
        PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
        PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
        PREFIX p1: <https://github.com/datasci2023/datascience/res/>
        PREFIX p2: <https://github.com/datasci2023/datascience/attr/>
        SELECT ?id ?label ?items
        WHERE {
            ?id rdf:type p1:Collection;
                rdfs:label ?label .
        }
        """
        df_sparql = get(endpoint, query, True)
        return df_sparql

    def getAllManifests(self) -> DataFrame:
        endpoint = self.getDbPathOrUrl()
        query = """
        PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
        PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
        PREFIX p1: <https://github.com/datasci2023/datascience/res/>
        PREFIX p2: <https://github.com/datasci2023/datascience/attr/>
        SELECT ?id ?label
        WHERE {
            ?id rdf:type p1:Manifest;
                rdfs:label ?label . 
                }
        """
        df_sparql = get(endpoint, query, True)
        return df_sparql

    def getCanvasesInCollection(self, collectionId: str) -> DataFrame:
        endpoint = self.getDbPathOrUrl()
        query = (
            """
        PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
        PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
        PREFIX p1: <https://github.com/datasci2023/datascience/res/>
        PREFIX p2: <https://github.com/datasci2023/datascience/attr/>
        SELECT ?canvas_id ?collection_id ?id ?manifest_id ?label 
        WHERE {
            ?id a '"""
            + collectionId
            + """'.
            ?collection_id rdf:type Collection;
                p2:items ?manifest_id .
            ?manifest_id p2:items ?canvas_id .
            ?canvas_id rdf:type p1:Canvas;
                rdfs:label ?label .                
        }
        """
        )
        df_sparql = get(endpoint, query, True)
        return df_sparql

    def getCanvasesInManifest(self, manifestId: str) -> DataFrame:
        endpoint = self.getDbPathOrUrl()
        query = (
            """
        PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
        PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
        PREFIX p1: <https://github.com/datasci2023/datascience/res/>
        PREFIX p2: <https://github.com/datasci2023/datascience/attr/>
        SELECT ?canvas_id ?id ?manifest_id ?label 
        WHERE {
            ?id a '"""
            + manifestId
            + """'.
            ?manifest_id p2:items ?canvas_id .
            ?canvas_id rdf:type p1:Canvas;
                rdfs:label ?label .                
        }
        """
        )
        df_sparql = get(endpoint, query, True)
        return df_sparql

    def getEntitiesWithLabel(self, label: str) -> DataFrame:
        endpoint = self.getDbPathOrUrl()
        query = (
            """
        PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
        PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
        PREFIX p1: <https://github.com/datasci2023/datascience/res/>
        PREFIX p2: <https://github.com/datasci2023/datascience/attr/>
        SELECT ?id ?items ?type
        WHERE {?id rdfs:label '"""
            + label
            + """';
                rdf:type ?type .
            OPTIONAL { ?id p2:items ?items}
        }
        """
        )

        df_sparql = get(endpoint, query, True)
        return df_sparql

    def getManifestsInCollection(self, collectionId) -> DataFrame:
        endpoint = self.getDbPathOrUrl()
        query = (
            """
        PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
        PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
        PREFIX p1: <https://github.com/datasci2023/datascience/res/>
        PREFIX p2: <https://github.com/datasci2023/datascience/attr/>
        SELECT ?manifest_id
        WHERE { ?id p2:items ?manifest_id . 
                FILTER ( ?id = <%s> ) 
                      
        }
        """
            % collectionId
        )

        df_sparql = get(endpoint, query, True)
        return df_sparql
