from impl import *

# from generic_test import GenericQueryProcessor

rel_path = "relational.db"
met_dp = MetadataProcessor()
met_dp.setDbPathOrUrl(rel_path)
met_dp.uploadData("data/metadata.csv")

ann_dp = AnnotationProcessor()
ann_dp.setDbPathOrUrl(rel_path)
ann_dp.uploadData("data/annotations.csv")

grp_endpoint = "http://127.0.0.1:9999/blazegraph/sparql"
col_dp = CollectionProcessor()
col_dp.setDbPathOrUrl(grp_endpoint)
col_dp.uploadData("data/collection-1.json")
col_dp.uploadData("data/collection-2.json")

rel_qp = RelationalQueryProcessor()
rel_qp.setDbPathOrUrl(rel_path)

grp_qp = TriplestoreQueryProcessor()
grp_qp.setDbPathOrUrl(grp_endpoint)

# Finally, create a generic query processor for asking
# about data
generic = GenericQueryProcessor()
generic.addQueryProcessor(rel_qp)
generic.addQueryProcessor(grp_qp)

# result = generic.getAllAnnotations()
# result = generic.getAllCanvas()
# result = generic.getAllCollections()
# result = generic.getAllImages()
# result = generic.getAllManifests()
# result = generic.getAnnotationsToCanvas(
#     "https://dl.ficlit.unibo.it/iiif/2/28429/canvas/p1"
# )
# result = generic.getAnnotationsToCollection(
#     "https://dl.ficlit.unibo.it/iiif/28429/collection"
# )
# result = generic.getAnnotationsToCollection(
#     "https://dl.ficlit.unibo.it/iiif/19428-19425/collection"
# )
# result = generic.getAnnotationsToManifest(
#     "https://dl.ficlit.unibo.it/iiif/2/28429/manifest"
# )
# result = generic.getAnnotationsWithBody(
#     "https://dl.ficlit.unibo.it/iiif/2/45498/full/699,800/0/default.jpg"
# )
# result = generic.getAnnotationsWithBodyAndTarget(
#     "https://dl.ficlit.unibo.it/iiif/2/45498/full/699,800/0/default.jpg",
#     "https://dl.ficlit.unibo.it/iiif/2/28429/canvas/p1",
# )
# result = generic.getAnnotationsWithTarget(
#     "https://dl.ficlit.unibo.it/iiif/2/28429/canvas/p1"
# )
# result = generic.getCanvasesInCollection(
#     "https://dl.ficlit.unibo.it/iiif/19428-19425/collection"
# )
# result = generic.getCanvasesInCollection(
#     "https://dl.ficlit.unibo.it/iiif/28429/collection"
# )
# result = generic.getCanvasesInManifest(
#     "https://dl.ficlit.unibo.it/iiif/2/28429/manifest"
# )
# result = generic.getCanvasesInManifest(
#     "https://dl.ficlit.unibo.it/iiif/2/19428/manifest"
# )
# result = generic.getCanvasesInManifest(
#     "https://dl.ficlit.unibo.it/iiif/2/19425/manifest"
# )
# result = generic.getEntityById("https://dl.ficlit.unibo.it/iiif/19428-19425/collection")
# result = generic.getEntityById("https://dl.ficlit.unibo.it/iiif/28429/collection")
# result = generic.getEntityById("https://dl.ficlit.unibo.it/iiif/2/28429/manifest")
# result1 = generic.getEntityById("https://dl.ficlit.unibo.it/iiif/2/19428/manifest")
# result2 = generic.getEntityById("https://dl.ficlit.unibo.it/iiif/2/19425/manifest")
# result3 = generic.getEntityById("https://dl.ficlit.unibo.it/iiif/2/19425/canvas/p1")
# result4 = generic.getEntityById("https://dl.ficlit.unibo.it/iiif/2/19")

# result = generic.getEntitiesWithCreator("Doe, Jane")
# result1 = generic.getEntitiesWithCreator("Doe, John")
# result2 = generic.getEntitiesWithCreator("Alighieri, Dante")
# result3 = generic.getEntitiesWithCreator("Raimondi, Giuseppe")
# result = generic.getEntitiesWithLabel("Works of Dante Alighieri")
# result = generic.getEntitiesWithLabel(
#     'Raimondi, Giuseppe. Quaderno manoscritto, "La vecchia centrale termica. Aprile 965"'
# )
result = generic.getEntitiesWithTitle("Dante Alighieri: Opere")
result1 = generic.getEntitiesWithTitle("Il Canzoniere")
result3 = generic.getEntitiesWithTitle(
    'Raimondi, Giuseppe. Quaderno manoscritto, "La vecchia centrale termica. Aprile 965"'
)
# result = generic.getImagesAnnotatingCanvas(
#     "https://dl.ficlit.unibo.it/iiif/2/28429/canvas/p1"
# )
# result = generic.getManifestsInCollection(
#     "https://dl.ficlit.unibo.it/iiif/28429/collection"
# )
# result1 = generic.getManifestsInCollection(
#     "https://dl.ficlit.unibo.it/iiif/19428-19425/collection"
# )

print(result)
print(result1)
print(result3)
