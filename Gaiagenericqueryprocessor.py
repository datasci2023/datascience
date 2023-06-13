from pandas import DataFrame
from queryProcessor import QueryProcessor
from data_model import *
from TriplestoreQueryProcessor import *
from RelationalQueryProcessor import *

class GenericQueryProcessor:
    def __init__(self):
        self.queryProcessor = []

    def cleanQueryProcessor(self):
        self.queryProcessor = []
        return True

    def addQueryProcessor(self, processor: QueryProcessor):
        self.queryProcessor.append(processor)
        return True
    
def getAllImages(self):
    result = []
    df_rel = DataFrame()
    
    for processor in self.queryProcessor:
        if isinstance(processor, RelationalQueryProcessor):
            df_rel = processor.getAllImages()

    for idx, row in df_rel.iterrows():
            entity = Image(
                row["image_ids"]
            ).fillna("")
            result.append(entity)

    return result

def getAnnotationsWithBody(self, bodyId):
    result = []
    df_rel = DataFrame()

    for processor in self.queryProcessor:
        if isinstance(processor, RelationalQueryProcessor):
            df_rel = processor.getAnnotationsWithBody(bodyId)

    for idx, row in df_rel.iterrows():
            entity = Annotation(
                row["annotation_ids"], row["annotation_motivations"], IdentifiableEntity(row["annotation_targets"]), Image(row["annotation_bodies"])
            ).fillna("")
            result.append(entity)

    return result

def getAnnotationsWithBodyAndTarget(self, bodyId:str, targetId:str):
    result = []
    df_rel = DataFrame()

    for processor in self.queryProcessor:
        if isinstance(processor, RelationalQueryProcessor):
            df_rel = processor.getAnnotationsWithBodyAndTarget(bodyId, targetId)

    for idx, row in df_rel.iterrows():
            entity = Annotation(
                row["annotation_ids"], row["annotation_motivations"], IdentifiableEntity(row["annotation_targets"]), Image(row["annotation_bodies"])
            ).fillna("")
            result.append(entity)

    return result

def getAnnotationsWithTarget(self, targetId:str ):
    result = []
    df_rel = DataFrame()

    for processor in self.queryProcessor:
        if isinstance(processor, RelationalQueryProcessor):
            df_rel = processor.getAnnotationsWithTarget(targetId)

    for idx, row in df_rel.iterrows():
            entity = Annotation(
                row["annotation_ids"], row["annotation_motivations"], IdentifiableEntity(row["annotation_targets"]), Image(row["annotation_bodies"])
            ).fillna("")
            result.append(entity)

    return result

def getImagesAnnotationgCanvas(self, canvasId):
    result = []
    df_rel = DataFrame()

    for processor in self.queryProcessor:
        if isinstance(processor, RelationalQueryProcessor):
            df_rel = processor.getImagesWithTarget(canvasId)

    for idx, row in df_rel.iterrows():
            entity = Image(
                row["image_ids"]
            ).fillna("")
            result.append(entity)

    return result

    



    
    

    


