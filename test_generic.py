from AnnotationMetadataProcessors import AnnotationProcessor, MetadataProcessor
from RelationalQueryProcessor import RelationalQueryProcessor
from collectionProcessor import CollectionProcessor
from TriplestoreQueryProcessor import TriplestoreQueryProcessor
from genericQueryProcessor import GenericQueryProcessor
from generic_creator import GenericQueryProcessorDeneme
from generic_test import GenericQueryProcessor

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

generic3 = GenericQueryProcessor()
generic3.addQueryProcessor(rel_qp)
generic3.addQueryProcessor(grp_qp)

# result = generic3.getAllAnnotations()
# result = generic3.getAllCanvas()
# result = generic3.getAllCollections()
# result = generic3.getAllImages()
# result = generic3.getAllManifests()
# result = generic3.getAnnotationsToCanvas(
#     "https://dl.ficlit.unibo.it/iiif/2/28429/canvas/p1"
# )
# result = generic3.getAnnotationsToCollection(
#     "https://dl.ficlit.unibo.it/iiif/28429/collection"
# )
# result = generic3.getAnnotationsToManifest(
#     "https://dl.ficlit.unibo.it/iiif/2/28429/manifest"
# )
# result = generic3.getAnnotationsWithBody(
#     "https://dl.ficlit.unibo.it/iiif/2/45498/full/699,800/0/default.jpg"
# )
# result = generic3.getAnnotationsWithBodyAndTarget(
#     "https://dl.ficlit.unibo.it/iiif/2/45498/full/699,800/0/default.jpg",
#     "https://dl.ficlit.unibo.it/iiif/2/28429/canvas/p1",
# )
# result = generic3.getAnnotationsWithTarget(
#     "https://dl.ficlit.unibo.it/iiif/2/28429/canvas/p1"
# )
result = generic3.getCanvasesInCollection(
    "https://dl.ficlit.unibo.it/iiif/28429/collectin"
)
# result = generic3.getCanvasesInManifest(
#     "https://dl.ficlit.unibo.it/iiif/2/28429/manifest"
# )
# result = generic3.getEntityById("https://dl.ficlit.unibo.it/iiif/2/28429/manifest")
# result = generic3.getEntitiesWithCreator("Doe, Jane")
# result = generic3.getEntitiesWithLabel("Works of Dante Alighieri")
# result = generic3.getEntitiesWithTitle("Dante Alighieri: Opere")
# result = generic3.getImagesAnnotatingCanvas(
#     "https://dl.ficlit.unibo.it/iiif/2/28429/canvas/p1"
# )
# result = generic3.getManifestsInCollection(
#     "https://dl.ficlit.unibo.it/iiif/28429/collection"
# )
print(result)
