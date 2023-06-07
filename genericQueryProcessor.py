from pandas import DataFrame, merge
from queryProcessor import QueryProcessor
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

    def getAllAnnotations():
        pass

    def getEntitiesWithLabel(self, label: str):
        result = list()
        df = DataFrame()

        for processor in self.queryProcessor:
            df = merge(processor, df, left_on="id", right_on="id")
            df_filled = df.fillna("")

        for idx, row in df_filled.iterrows():
            id = row["id"]
            label = label
            if row["title"]:
                title = row["title"]
                creators = row["creators"]
            else:
                title = ""
                creators = ""

            entity = EntityWithMetadata(id, label, title, creators)
            result.append(entity)

        return result
