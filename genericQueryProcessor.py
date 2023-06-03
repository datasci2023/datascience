from pandas import DataFrame, concat

from queryProcessor import QueryProcessor


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

    def getAllCanvas(self):
        result = set()
        canvas = {}
        df = DataFrame()
        
        for processor in self.queryProcessor:
            df = concat(processor, df)
            
        for idx,rows in df.iterrows():