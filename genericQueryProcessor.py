from pandas import DataFrame, merge, concat
from processor import Processor
from RelationalQueryProcessor import RelationalQueryProcessor
from TriplestoreQueryProcessor import TriplestoreQueryProcessor
from queryProcessor import QueryProcessor
from data_model import *


class GenericQueryProcessor:
    def __init__(self):
        self.queryProcessor = [QueryProcessor]

    def cleanQueryProcessor(self):
        self.queryProcessor = []
        return True

    def addQueryProcessor(self, processor: QueryProcessor):
        self.queryProcessor.append(processor)
        return True

    def getAllCanvas(self):
        result = list()
        df_graph = DataFrame()
        df_rel = DataFrame()
        joined_df = DataFrame()

        for processor in self.queryProcessor:
            if isinstance(processor, TriplestoreQueryProcessor):
                df_graph = processor.getAllCanvases()

        for processor in self.queryProcessor:
            if isinstance(processor, RelationalQueryProcessor):
                for idx, row in df_graph.iterrows():
                    df_rel.append(processor.getEntitybyId(row[id]))
                    # joined_df.append(df_rel)

        joined_df = (
            merge(df_rel, df_graph, left_on="id", right_on="id")
            .fillna("")
            .drop_duplicates()
        )

        for idx, row in joined_df.iterrows():
            entity = Canvas(
                row["id"], row["label"], row["title"], row["creator"]
            ).fillna("")
            result.append(entity)

        return result

    def getCanvasesInCollection(self, collectionId: str):
        result = list()
        df_graph = DataFrame()
        df_rel = DataFrame()
        joined_df = DataFrame()

        for processor in self.queryProcessor:
            if isinstance(processor, TriplestoreQueryProcessor):
                df_graph = processor.getCanvasesInCollection(collectionId)

        for processor in self.queryProcessor:
            if isinstance(processor, RelationalQueryProcessor):
                for idx, row in df_graph.iterrows():
                    df_rel.append(processor.getEntitybyId(row[id]))
                    # joined_df.append(df_rel)

        joined_df = (
            merge(df_rel, df_graph, left_on="id", right_on="id")
            .fillna("")
            .drop_duplicates()
        )

        for idx, row in joined_df.iterrows():
            entity = Canvas(
                row["id"], row["label"], row["title"], row["creator"]
            ).fillna("")
            result.append(entity)

        return result

    def getCanvasesInManifest(self, manifestId: str):
        result = list()
        df_graph = DataFrame()
        df_rel = DataFrame()
        joined_df = DataFrame()

        for processor in self.queryProcessor:
            if isinstance(processor, TriplestoreQueryProcessor):
                df_graph = processor.getCanvasesInCollections(manifestId)

        for processor in self.queryProcessor:
            if isinstance(processor, RelationalQueryProcessor):
                for idx, row in df_graph.iterrows():
                    df_rel.append(processor.getEntitybyId(row[id]))
                    # joined_df.append(df_rel)

        joined_df = (
            merge(df_rel, df_graph, left_on="id", right_on="id")
            .fillna("")
            .drop_duplicates()
        )

        for idx, row in joined_df.iterrows():
            entity = Canvas(
                row["id"], row["label"], row["title"], row["creator"]
            ).fillna("")
            result.append(entity)

        return result

    def getEntitiesWithLabel(self, label: str):
        result = list()
        df_graph = DataFrame()
        df_rel = DataFrame()
        joined_df = DataFrame()

        for processor in self.queryProcessor:
            if isinstance(processor, TriplestoreQueryProcessor):
                df_graph = processor.getEntitiesWithLabel(label)

        for processor in self.queryProcessor:
            if isinstance(processor, RelationalQueryProcessor):
                for idx, row in df_graph.iterrows():
                    df_rel.append(processor.getEntitybyId(row[id]))
                    # joined_df.append(df_rel)

        joined_df = (
            merge(df_rel, df_graph, left_on="id", right_on="id")
            .fillna("")
            .drop_duplicates()
        )

        for idx, row in joined_df.iterrows():
            entity = EntityWithMetadata(
                row["id"], label, row["title"], row["creator"]
            ).fillna("")
            result.append(entity)

        return result

    def getEntitiesWithTitle(self, title):
        result = list()
        df_graph = DataFrame()
        df_rel = DataFrame()
        joined_df = DataFrame()

        for processor in self.queryProcessor:
            if isinstance(processor, RelationalQueryProcessor):
                df_graph = processor.getEntitiesWithTitle(title)

        for processor in self.queryProcessor:
            if isinstance(processor, TriplestoreQueryProcessor):
                for idx, row in df_graph.iterrows():
                    df_rel.append(processor.getEntitybyId(row[id]))
                    # joined_df.append(df_rel)

        joined_df = (
            merge(df_rel, df_graph, left_on="id", right_on="id")
            .fillna("")
            .drop_duplicates()
        )

        for idx, row in joined_df.iterrows():
            entity = EntityWithMetadata(
                row["id"], row["label"], title, row["creator"]
            ).fillna("")
            result.append(entity)
