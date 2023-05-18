from pandas import DataFrame
from processor import Processor
from queryProcessor import queryProcessor
import pandas as pd 
# from sparql_dataframe import get
from pandas.io.json import json_normalize
from SPARQLWrapper import SPARQLWrapper, JSON

#remember the strucutre realted to "label" : the way it's done now, it's going to return only the "none"

class TriplestoreQueryProcessor(queryProcessor):
    def __init__(self, entityId):
        super().__init__(entityId)

    def getAllCanvases(self, query, endpoint) -> DataFrame:
        #endpoint = "http://127.0.0.1:9999/blazegraph/sparql"
        endpoint = Processor.getDbPathOrUrl
        query = """
        PREFIX rdf ??? 
        SELECT ?id ?label ?canvas
        WHERE {
            ?id rdf:type ?canvas a "Canvas" ;
            rdfs:label ?label .
        }
        """
        # df_sparql = get(endpoint, query, True)
        # return df_sparql
        sparql = SPARQLWrapper(endpoint)
        sparql.setQuery(query) 
        sparql.setReturnFormat(JSON)
        result = sparql.query().convert()
        return json_normalize(result["type"]["canvas"]) #???


    def getAllCollections(self, query, endpoint) -> DataFrame:
        #endpoint = "http://127.0.0.1:9999/blazegraph/sparql"
        endpoint = Processor.getDbPathOrUrl
        #do I have to add also a query related to the data of the "items" (???)
        query = """
        PREFIX rdf ??? 
        SELECT ?id ?label ?collection
        WHERE {
            ?id rdf:type ?collection a "Collection" ;
            rdfs:label ?label .
        }
        """
        # df_sparql = get(endpoint, query, True)
        # return df_sparql
        sparql = SPARQLWrapper(endpoint)
        sparql.setQuery(query) 
        sparql.setReturnFormat(JSON)
        result = sparql.query().convert()
        return json_normalize(result["type"]["collection"]) #???

    def getAllManifests(self, query, endpoint) -> DataFrame:
        #endpoint = "http://127.0.0.1:9999/blazegraph/sparql"
        endpoint = Processor.getDbPathOrUrl
        query = """
        PREFIX rdf ??? 
        SELECT ?id ?label ?manifest
        WHERE {
            ?id rdf:type ?manifest a "Manifest" ;
            rdfs:label ?label .
        }
        """
        # df_sparql = get(endpoint, query, True)
        # return df_sparql
        sparql = SPARQLWrapper(endpoint)
        sparql.setQuery(query) 
        sparql.setReturnFormat(JSON)
        result = sparql.query().convert()
        return json_normalize(result["type"]["manifest"]) #???

    def getCanvasesInCollection(self, query, endpoint, collectionId:str) -> DataFrame:
        # endpoint = "http://127.0.0.1:9999/blazegraph/sparql"
        endpoint = Processor.getDbPathOrUrl
        query = """
        PREFIX rdf ??? 
        SELECT ?id ?label ?type
        WHERE {
            ?id rdf:items ?manifest .
            ?id a \""""+collectionId+"""\ .
            ?manifest rdf:items ?canvas .
            ?canvas rdf:type ?type a "Canvas"; 
            rdfs:label ?label .
        }
        """
        # df_sparql = get(endpoint, query, True)
        # return df_sparql
        sparql = SPARQLWrapper(endpoint)
        sparql.setQuery(query) 
        sparql.setReturnFormat(JSON)
        result = sparql.query().convert()
        return json_normalize(result["type"]["canvas"]) #???

    def getCanvasesInManifest(manifestId:str) -> DataFrame:
        #same as above but for manifest
        pass

    def getEntitiesWithLabel(label:str) -> DataFrame:
        #returns dataframe with all metadata related to entities with as label the input label
        pass

    def getManifestsInCollection(collectionId:str) -> DataFrame:
        #same as canvases in manifest
        pass

    