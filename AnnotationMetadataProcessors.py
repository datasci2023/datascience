from data_model import *
from processor import Processor

from sqlite3 import connect
from pandas import read_csv, Series, DataFrame, merge
import pandas as pd


class MetadataProcessor(Processor):
    def __init__(self, dbPathOrUrl : str):
        super(Processor).__init__(dbPathOrUrl)
        # self.dbPathOrUrl = None

    def uploadData(self, path):
        try:
            with connect("relationaldatabase.db") as con:

                path = read_csv("C:\\Users\\chiar\\Documents\\GitHub\\datascience\\metadata.csv",
                                keep_default_na=False,
                                dtype={
                                    "id": "string",
                                    "title": "string",
                                    "creator": "string"
                                })

                # CREATORS
                creators = pd.DataFrame()
                creator_list = []
                for value in path["creator"]:
                    if value != "":
                            creator_list.append(value)
                creators.insert(0,"creator", Series(creator_list, dtype= "string"))
                creators["creator"] = creators["creator"].str.split(";") 
                # Separate values using the separator ";"
                #creators["creator"] = creators["creator"].str.strip()
                # Eliminate blank spaces at the beginning and at the end; it creates problem I can't understand why
                creators_def = creators.explode("creator")
                # Expansion of the column in separate rows
                creators_def = creators_def.reset_index(drop=True)
                #Update of the index; " drop=True " removes the previous index without creating a new column with the previous index
                internal_id_dict = {}
                creator_internal_id = []

                for idx, row in creators_def.iterrows():
                    creator = row["creator"]
                    
                    if creator in internal_id_dict:
                        creator_internal_id.append(internal_id_dict[creator])
                    else:
                        internal_id = "creator-" + str(len(internal_id_dict))
                        creator_internal_id.append(internal_id)
                        internal_id_dict[creator] = internal_id
                
                creators_def.insert(0,"creator_internal_id", Series(creator_internal_id, dtype= "string"))
                 


                # ENTITY WITH METADATA
                metadata_entities = path[["id", "title", "creator"]]
                metadata_entities["creator"] = metadata_entities["creator"].str.split(";") 
                # Separate values using the separator ";"
                #metadata_entities["creator"] = metadata_entities["creator"].str.strip()
                # Eliminate blank spaces at the beginning and at the end; it creates problem I can't figure out why
                metadata_entities = metadata_entities.explode("creator")
                # Expansion of the column in separate rows
                metadata_entities = metadata_entities.reset_index(drop=True)
                # Update of the index; " drop=True " removes the previous index without creating a new column with the previous index
                #metadata_entities["creator"] = metadata_entities["creator"].replace(creators.set_index("creator")["creator_internal_id"])
                # Check whether it's better to use .replace or .merge
                metadata_internal_id = []
                for idx, row in metadata_entities.iterrows():
                        metadata_internal_id.append("metadata-" + str(idx))
                metadata_entities.insert(0,"metadata_internal_id", Series(metadata_internal_id, dtype= "string")) 

                metadata_entities.to_sql("EntitiesWithMetadata", con, if_exists="replace", index=False)
                creators_def.to_sql("Creators", con, if_exists="replace", index=False) 

                con.commit()

                return True

        except Exception as e:
            print("Errore durante il commit delle modifiche:")
            print(e)
            return False
           
class AnnotationProcessor(Processor):
    def __init__(self, dbPathOrUrl : str):
        super(Processor).__init__(dbPathOrUrl)
        # self.dbPathOrUrl = None
    
    def load_dataframe_from_db(self):
        if "relationaldatabase.db":
            with connect("relationaldatabase.db") as con:
                query = "SELECT * FROM EntitiesWithMetadata"
                metadata_temp = pd.read_sql_query(query, con)
                return metadata_temp
                # Load EntityWithMetadata dataframe from the database so that it can be merged with dataframes created with uploadData method   

    def uploadData(self, path2):            
        try:
            metadata_temp = self.load_dataframe_from_db(self=AnnotationProcessor)        
            with connect("relationaldatabase.db") as con:
            #ANNOTATION TABLE
                path2 = read_csv('C:\\Users\\chiar\\Documents\\GitHub\\datascience\\annotations.csv', # path has to be updated to a generic path
                                keep_default_na=False,
                                dtype={
                                    "id": "string",
                                    "body": "string",
                                    "target": "string",
                                    "motivation": "string",
                                })             
                 

                annotation_table = pd.DataFrame()
                annotation_ids = []
                for idx, value in path2['id'].items():
                    annotation_ids.append(value)

                annotation_table.insert(0,"annotation_ids", Series(annotation_ids, dtype= "string"))

                annotation_internal_id = []
                for idx, row in  path2.iterrows():
                    annotation_internal_id.append("annotation-" + str(idx))

                annotation_table.insert(0, "annotation_internal_id", Series(annotation_internal_id, dtype="string"))

                annotation_bodies = []
                for idx, value in path2['body'].items():
                    annotation_bodies.append(value)

                annotation_table.insert(2,"annotation_bodies", Series(annotation_bodies, dtype= "string"))

                annotation_targets = []
                for idx, value in path2['target'].items():
                    annotation_targets.append(value)

                annotation_table.insert(3,"annotation_targets", Series(annotation_targets, dtype= "string"))
                annotation_merged = merge(annotation_table, metadata_temp, left_on="annotation_targets", right_on="id")
                # Merge Annotations table with the temporary metadata table, using as key the id of the entity with metadata
                annotations = annotation_merged[["annotation_ids", "annotation_internal_id", "annotation_bodies", "metadata_internal_id"]]
                # Keep only the columns we need
                annotations = annotations.rename(columns={"metadata_internal_id": "annotation_targets"})
                # Use the column with the entity with metadata internal id, instead of just the id; rename it as "annotation_targets"

                
                annotation_motivations = []
                for idx, value in path2['motivation'].items():
                    annotation_motivations.append(value)

                annotations.insert(4,"annotation_motivations", Series(annotation_motivations, dtype= "string"))


                #IMAGE TABLE

                image = pd.DataFrame()
                image_ids = []
                for index, value in path2["body"].items():
                    image_ids.append(value)

                image.insert(0, "image_ids", Series(image_ids, dtype="string" ))
                    


                image_internal_id = []
                for idx, rows in image.iterrows():
                    image_internal_id.append("images-" + str(idx))

                image.insert(0, "images_internal_id", Series(image_internal_id, dtype="string"))

                annotation_merged2 = merge(annotations, image, left_on="annotation_bodies", right_on="image_ids")
                # Merge Annotations table with Images table, using as key the id of the image
                annotation_def = annotation_merged2[["annotation_internal_id", "annotation_ids", "images_internal_id", "annotation_targets", "annotation_motivations"]]
                # Keep only the columns we need
                annotation_def = annotation_def.rename(columns={"images_internal_id": "annotation_bodies"})
                # Use the column with the image internal id, instead of just the id; rename it as "annotation_bodies"
                # Alternative method: annotation_table["annotation_bodies"] = annotation_table["annotation_bodies"].replace(image.set_index("image_ids")["images_internal_id"])


                annotation_def.to_sql("Annotations", con, if_exists="replace", index=False)
                image.to_sql("Images", con, if_exists="replace", index=False)


                con.commit()

                return True

        except Exception as e:
            print(e)
            return False
        
MetadataProcessor.uploadData(self=MetadataProcessor, path="C:\\Users\\chiar\\Documents\\GitHub\\datascience\\metadata.csv")
AnnotationProcessor.load_dataframe_from_db(self=AnnotationProcessor)
AnnotationProcessor.uploadData(self=AnnotationProcessor, path2="C:\\Users\\chiar\\Documents\\GitHub\\datascience\\annotations.csv")