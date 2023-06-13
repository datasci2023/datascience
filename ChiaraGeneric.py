from pandas import DataFrame, merge, concat
from queryProcessor import QueryProcessor
from data_model import *
from TriplestoreQueryProcessor import *
from RelationalQueryProcessor import *

class GenericQueryProcessor:
    def __init__(self):
        self.queryProcessors = []

    def addQueryProcessor(self, processor: QueryProcessor):
        self.queryProcessors.append(processor)
        return True

    def getAllAnnotations(self):
        result = list()
        df_rel = DataFrame()

        for processor in self.queryProcessors:
            if isinstance(processor, RelationalQueryProcessor):
                df_rel = processor.getAllAnnotations()

        for idx, row in df_rel.iterrows():
            entity = Annotation(
                row["annotation_ids"], 
                row["annotation_motivations"],
                IdentifiableEntity(row['annotation_targets']), 
                Image(row['annotation_bodies'])
                )#.fillna("")
            result.append(entity)
            
        return result
    
    def getAnnotationsToManifest(self, manifestId: str):
        result = list()
        df_rel = DataFrame()

        for processor in self.queryProcessors:
            if isinstance(processor, RelationalQueryProcessor):
                df_rel = processor.getAnnotationsWithTarget(manifestId)

        for idx, row in df_rel.iterrows():
            entity = Annotation(
                row["annotation_ids"], 
                row["annotation_motivations"],
                IdentifiableEntity(row['annotation_targets']),
                Image(row['annotation_bodies'])
                )#.fillna("")
            result.append(entity)
            
        return result

    def getAnnotationsToCollection(self, collectionId: str):
        result = list()
        df_rel = DataFrame()

        for processor in self.queryProcessors:
            if isinstance(processor, RelationalQueryProcessor):
                df_rel = processor.getAnnotationsWithTarget(collectionId)

        for idx, row in df_rel.iterrows():
            entity = Annotation(
                row["annotation_ids"], 
                row["annotation_motivations"],
                IdentifiableEntity(row['annotation_targets']), 
                Image(row['annotation_bodies'])
                )#.fillna("")
            result.append(entity)
            
        return result

    def getAnnotationsToManifest(self, manifestId: str):
        result = list()
        df_rel = DataFrame()

        for processor in self.queryProcessors:
            if isinstance(processor, RelationalQueryProcessor):
                df_rel = processor.getAnnotationsWithTarget(manifestId)

        for idx, row in df_rel.iterrows():
            entity = Annotation(
                row["annotation_ids"], 
                row["annotation_motivations"],
                IdentifiableEntity(row['annotation_targets']), 
                Image(row['annotation_bodies'])
                )#.fillna("")
            result.append(entity)
            
        return result

    def getAnnotationsToCanvas(self, canvasId: str):
        result = list()
        df_rel = DataFrame()

        for processor in self.queryProcessors:
            if isinstance(processor, RelationalQueryProcessor):
                df_rel = processor.getAnnotationsWithTarget(canvasId)

        for idx, row in df_rel.iterrows():
            entity = Annotation(
                row["annotation_ids"], 
                row["annotation_motivations"],
                IdentifiableEntity(row['annotation_targets']), 
                Image(row['annotation_bodies'])
                )#.fillna("")
            result.append(entity)
            
        return result