#class AnnotationProcessor(object): # la superclasse è il Processor di Lucia
    #def uploadData():

from sqlite3 import connect
from pandas import read_csv, Series
import pandas as pd


with connect("my_relationaldabase.db") as con:
#ANNOTATION TABLE
    annotation = read_csv('C:\\Users\\HP\\OneDrive\\Desktop\\CARTELLAPROVADATASCIENCE\\annotations.csv',
                       keep_default_na=False,
                       dtype={
                           "id": "string",
                           "body": "string",
                           "target": "string",
                           "motivation": "string",
                       })

annotation_table = pd.DataFrame()
annotation_ids = []
for idx, value in annotation['id'].items():
    annotation_ids.append(value)

annotation_table.insert(0,"annotation_ids", Series(annotation_ids, dtype= "string"))

annotation_targets = []
for idx, value in annotation['target'].items():
    annotation_targets.append(value)

annotation_table.insert(1,"annotation_targets", Series(annotation_targets, dtype= "string"))

annotation_motivations = []
for idx, value in annotation['motivation'].items():
    annotation_motivations.append(value)

annotation_table.insert(2,"annotation_motivations", Series(annotation_motivations, dtype= "string"))

annotation_internal_id = []
for idx, rows in  annotation.iterrows():
    annotation_internal_id.append("annotation-" + str(idx))

annotation_table.insert(3, "annotation_internal_id", Series(annotation_internal_id, dtype="string"))



#IMAGE TABLE

image = pd.DataFrame()
image_ids = []
for index, value in annotation["body"].items():
    image_ids.append(value)

image.insert(0, "image_ids", Series(image_ids, dtype="string" ))
    


image_internal_id = []
for idx, rows in image.iterrows():
     image_internal_id.append("images-" + str(idx))

image.insert(1, "images_internal_id", Series(image_internal_id, dtype="string"))

annotation_table.insert(4, "annotation_bodies", Series(image_internal_id, dtype="string"))


annotation_table.to_sql("annotation", con, if_exists="replace", index=False)
image.to_sql("Image", con, if_exists="replace", index=False)

con.commit()



