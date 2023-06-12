from pandas import DataFrame, merge
from RelationalQueryProcessor import *
from TriplestoreQueryProcessor import *
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
        df = DataFrame()
        joined_df = DataFrame()

        for processor in self.queryProcessor:
            if isinstance(processor, RelationalQueryProcessor):
                df = processor.getAllEntities()
            elif isinstance(processor, TriplestoreQueryProcessor):
                df = processor.getEntitiesWithLabel()
            # rethink the else case

            joined_df = (
                merge(joined_df, df, left_on="id", right_on="id")
                .fillna("")
                .drop_duplicates()
            )

        # joined_df["creator"] = joined_df.groupby(
        #     ["canvas", "id", "label", "entityId", "title"]
        # )["creator"].transform(lambda x: "; ".join(x))
        # joined_df = joined_df[
        #     ["canvas", "id", "label", "title", "creator"]
        # ].drop_duplicates()

        for idx, row in joined_df.iterrows():
            entity = Canvas(
                row["id"], row["label"], row["title"], row["creators"]
            ).fillna("")
            result.append(entity)

        return result

    def getCanvasesInCollection(self):
        result = list()
        df = DataFrame()
        joined_df = DataFrame()

        for processor in self.queryProcessor:
            if isinstance(processor, RelationalQueryProcessor):
                df = processor.getAllEntities()
            elif isinstance(processor, TriplestoreQueryProcessor):
                df = processor.getEntitiesWithLabel()

        joined_df = (
            merge(joined_df, df, left_on="id", right_on="id")
            .fillna("")
            .drop_duplicates()
        )

        for idx, row in joined_df.iterrows():
            entity = Canvas(
                row["id"], row["label"], row["title"], row["creator"]
            ).fillna("")
            result.append(entity)

        return result

    def getCanvasesInManifest():
        result = list()
        df = DataFrame()
        joined_df = DataFrame()

        for processor in self.queryProcessor:
            if isinstance(processor, RelationalQueryProcessor):
                df = processor.getAllEntities()
            elif isinstance(processor, TriplestoreQueryProcessor):
                df = processor.getEntitiesWithLabel()

            joined_df = (
                merge(joined_df, df, left_on="id", right_on="id")
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
        df = DataFrame()
        joined_df = DataFrame()

        for processor in self.queryProcessor:
            if isinstance(processor, RelationalQueryProcessor):
                df = processor.getAllEntities()
            elif isinstance(processor, TriplestoreQueryProcessor):
                df = processor.getEntitiesWithLabel()
            # rethink the else case

            joined_df = (
                merge(joined_df, df, left_on="id", right_on="id")
                .fillna("")
                .drop_duplicates()
            )

        for idx, row in joined_df.iterrows():
            id = row["id"]
            label = label
            if row["title"]:
                title = row["title"]
                creators = row["creators"]
            else:
                title = ""
                creators = ""

            entity = EntityWithMetadata(row["id"], label, title, creators)
            result.append(entity)

        return result


def getEntitiesWithTable():
    pass
