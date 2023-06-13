from pandas import DataFrame, merge
from queryProcessor import QueryProcessor
from TriplestoreQueryProcessor import TriplestoreQueryProcessor
from RelationalQueryProcessor import RelationalQueryProcessor
from data_model import *

class GenericQueryProcessor:
    def __init__(self):
        self.queryProcessor = []

    def cleanQueryProcessor(self):
        self.queryProcessor = []
        return True

    def addQueryProcessor(self, processor: QueryProcessor):
        self.queryProcessor.append(processor)
        return True

    def getAllCollections(self) -> list(Collection):
        result = list()
        df_graph = DataFrame()
        df_rel = DataFrame()
        joined_df = DataFrame()

        for processor in self.queryProcessor:
            if isinstance(processor, TriplestoreQueryProcessor):
                df_graph = processor.getAllCollections()

        for processor in self.queryProcessor:
            if isinstance(processor, RelationalQueryProcessor):
                for idx, row in df_graph.iterrows():
                    df_rel.append(processor.getEntityById(row[id]))

        joined_df = merge(df_rel, df_graph, left_on="id", right_on="id").fillna("")       

        for idx, row in joined_df.iterrows():
            entity = Collection(row["id"], row["label"], row["title"]) #row["items"], row["creator"] TO ADD TO SINGLE CELL 

        result.append(entity)
        return result 

    def getAllManifests(self) -> list(Manifest):
        result = list()
        df_graph = DataFrame()
        df_rel = DataFrame()
        joined_df = DataFrame()
        
        for processor in self.queryProcessor:
            if isinstance(processor, TriplestoreQueryProcessor):
                df_graph = processor.getAllManifests()

        for processor in self.queryProcessor:
            if isinstance(processor, RelationalQueryProcessor):
                for idx, row in df_graph.iterrows():
                    df_rel.append(processor.getEntityById(row[id]))

        joined_df = merge(df_rel, df_graph, left_on="id", right_on="id").fillna("")

        for idx, row in joined_df.iterrows():
            entity = Manifest(row["id"], row["label"], row["title"]) #row["creator"], row["items"]) TO ADD TO SINGLE CELL

        result.append(entity)
        return result

    def getEntityById(self, entityId:str) -> IdentifiableEntity or None: 
        result = list()
        df_graph = DataFrame()
        df_rel = DataFrame()
        
        for processor in self.queryProcessor:
            if isinstance(processor, TriplestoreQueryProcessor):
                df_graph = processor.getEntityById(entityId)
                for idx, row in df_graph.iterrows():
                    entity = IdentifiableEntity(row["id"])
                    result.append(entity)
                return result
            elif isinstance(processor, RelationalQueryProcessor):
                df_rel = processor.getEntityById(entityId)
                for idx, row in df_rel.iterrows():
                    entity = IdentifiableEntity(row["id"])
                    result.append(entity)
                return result
            else:
                return None

    def EntitiesWithCreator(self, creatorName:str) -> list(EntityWithMetadata):
        result = list()
        df_rel = DataFrame()
        df_graph = DataFrame()
        joined_df = DataFrame()
        
        for processor in self.queryProcessor:
            if isinstance(processor, RelationalQueryProcessor):
                df_rel = processor.getEntitiesWithCreator(creatorName)

        for processor in self.queryProcessor:
            if isinstance(processor, TriplestoreQueryProcessor):
                for idx, row in df_graph.iterrows():
                    df_graph.append(processor.getEntitybyId(row[id]))

        joined_df = merge(df_rel, df_graph, left_on="id", right_on="id").fillna("")

        for idx, row in joined_df.iterrows():
            entity = EntityWithMetadata(row["id"], row["label"], row["title"], row["creator"])

        result.append(entity)
        return result

    def getManifestsInCollection(self, collectionId:str) -> list(Manifest):
        result = list()
        df_graph = DataFrame()
        df_rel = DataFrame()
        joined_df = DataFrame()

        for processor in self.queryProcessor:
            if isinstance(processor, TriplestoreQueryProcessor):
                df_graph = processor.getManifestsInCollection(collectionId)

        for processor in self.queryProcessor:
            if isinstance(processor, RelationalQueryProcessor):
                for idx, row in df_graph.iterrows():
                    df_rel.append(processor.getEntitybyId(row[id]))

        joined_df = merge(df_rel, df_graph, left_on="id", right_on="id").fillna("")

        for idx, row in joined_df.iterrows():
            entity = Manifest(row["id"], row["label"], row["title"], row["creator"]) #row["items"] CREATE ONLY ONE CELL 

        result.append(entity)
        return result
