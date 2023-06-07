from pandas import DataFrame, concat

from queryProcessor1 import QueryProcessor
from data_model import *


class GenericQueryProcessor:
    def __init__(self, queryProcessors):
        self.queryProcessors = 

    def cleanQueryProcessor(self):
        self.queryProcessor = []
        return True

    def addQueryProcessor(self, processor: QueryProcessor):
        self.queryProcessor.append(processor)
        return True

    def getAllAnnotations(self):
        annotations_list = []
        for item in self.df_set:
            for value in item.iterrows(): 
                if "/annotation/" in value:
                    annotation = Annotation(item["annotation_ids"], item["annotation_motivations"], item["id"], item["image_ids"])
                    annotations_list.append(annotation)


    def getAllCanvas(self):
        result = set()
        canvas = {}
        df = DataFrame()
        
        for processor in self.queryProcessor:
            df = concat(processor, df)
            
        for idx,rows in df.iterrows():
            pass

print(QueryProcessor.getEntityById(self=QueryProcessor, entityId="https://dl.ficlit.unibo.it/iiif/2/45498/full/699,800/0/default.jpg"))
print(GenericQueryProcessor.getAllAnnotations(self= GenericQueryProcessor, df_set=(QueryProcessor.getEntityById(self=QueryProcessor, entityId="https://dl.ficlit.unibo.it/iiif/2/45498/full/699,800/0/default.jpg"))))