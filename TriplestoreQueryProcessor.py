from pandas import DataFrame
from queryProcessor import QueryProcessor
from sparql_dataframe import get


class TriplestoreQueryProcessor(QueryProcessor):
    def __init__(self):
        super().__init__()

    def getAllCanvases(self) -> DataFrame:  # tested
        endpoint = self.getDbPathOrUrl()
        query = """
        PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
        PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
        PREFIX pomodoro: <https://github.com/datasci2023/datascience/class/>
        PREFIX feslegen: <https://github.com/datasci2023/datascience/attribute/>
        PREFIX spaghetti:  <https://github.com/datasci2023/datascience/relation/>

        SELECT ?id ?label
        WHERE {
            ?id rdf:type pomodoro:Canvas;
                rdfs:label ?label .
        }
        """
        df_sparql = get(endpoint, query, True)
        return df_sparql

    def getAllCollections(self) -> DataFrame:  # tested
        endpoint = self.getDbPathOrUrl()
        query = """
        PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
        PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
        PREFIX pomodoro: <https://github.com/datasci2023/datascience/class/>
        PREFIX feslegen: <https://github.com/datasci2023/datascience/attribute/>
        PREFIX spaghetti:  <https://github.com/datasci2023/datascience/relation/>
        
        SELECT ?id ?label ?manifest_id
        WHERE {
            ?id rdf:type pomodoro:Collection;
                rdfs:label ?label ;
                spaghetti:items ?manifest_id .
        }
        """
        df_sparql = get(endpoint, query, True)
        return df_sparql

    def getAllManifests(self) -> DataFrame:  # tested
        endpoint = self.getDbPathOrUrl()
        query = """
        PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
        PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
        PREFIX pomodoro: <https://github.com/datasci2023/datascience/class/>
        PREFIX feslegen: <https://github.com/datasci2023/datascience/attribute/>
        PREFIX spaghetti:  <https://github.com/datasci2023/datascience/relation/>
        
        SELECT ?id ?label ?canvas_id
        WHERE {
            ?id rdf:type pomodoro:Manifest;
                rdfs:label ?label ;
                spaghetti:items ?canvas_id . 
        }
        """
        df_sparql = get(endpoint, query, True)
        return df_sparql

    def getCanvasesInCollection(self, collectionId: str) -> DataFrame:  # tested
        endpoint = self.getDbPathOrUrl()
        query = (
            """
        PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
        PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
        PREFIX pomodoro: <https://github.com/datasci2023/datascience/class/>
        PREFIX feslegen: <https://github.com/datasci2023/datascience/attribute/>
        PREFIX spaghetti:  <https://github.com/datasci2023/datascience/relation/>
        
        SELECT ?id ?label 
        WHERE {
            ?collection_id rdf:type pomodoro:Collection ; 
                spaghetti:items ?manifest_id .
            ?manifest_id rdf:type pomodoro:Manifest;
                spaghetti:items ?id .
            ?id rdf:type pomodoro:Canvas;
                rdfs:label ?label .    
            
            FILTER(?collection_id = <%s> )            
        } 
        """
            % collectionId
        )
        df_sparql = get(endpoint, query, True)
        return df_sparql

    def getCanvasesInManifest(self, manifestId: str) -> DataFrame:  # tested
        endpoint = self.getDbPathOrUrl()
        query = (
            """
        PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
        PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
        PREFIX pomodoro: <https://github.com/datasci2023/datascience/class/>
        PREFIX feslegen: <https://github.com/datasci2023/datascience/attribute/>
        PREFIX spaghetti:  <https://github.com/datasci2023/datascience/relation/>
        
        SELECT ?id ?label 
        WHERE {
            ?manifest_id rdf:type pomodoro:Manifest;
                spaghetti:items ?id .
            ?id rdf:type pomodoro:Canvas;
                rdfs:label ?label .     
            FILTER ( ?manifest_id = <%s> ) 
           
        }
        """
            % manifestId
        )
        df_sparql = get(endpoint, query, True)
        return df_sparql

    def getEntitiesWithLabel(self, label: str) -> DataFrame:
        endpoint = self.getDbPathOrUrl()
        query = (
            """
        PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
        PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
        PREFIX pomodoro: <https://github.com/datasci2023/datascience/class/>
        PREFIX feslegen: <https://github.com/datasci2023/datascience/attribute/>
        PREFIX spaghetti:  <https://github.com/datasci2023/datascience/relation/>
        
        SELECT ?id ?type ?label
        WHERE { 
            ?id rdfs:label ?label;
                rdf:type ?type .
            FILTER ( ?label = "%s" ) 
        }
        """
            % label
        )

        df_sparql = get(endpoint, query, True)
        return df_sparql

    def getManifestsInCollection(self, collectionId) -> DataFrame:
        endpoint = self.getDbPathOrUrl()
        query = (
            """
        PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
        PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
        PREFIX pomodoro: <https://github.com/datasci2023/datascience/class/>
        PREFIX feslegen: <https://github.com/datasci2023/datascience/attribute/>
        PREFIX spaghetti:  <https://github.com/datasci2023/datascience/relation/>
        
        SELECT ?id ?label ?canvas_id
        WHERE { 
            ?collection_id rdf:type pomodoro:Collection;
                spaghetti:items ?id . 
            ?id rdfs:label ?label ;
                spaghetti:items ?canvas_id .
            FILTER ( ?collection_id = <%s> ) 
                      
        }
        """
            % collectionId
        )

        df_sparql = get(endpoint, query, True)
        return df_sparql
