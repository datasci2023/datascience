from AnnotationMetadataProcessors import AnnotationProcessor, MetadataProcessor
from RelationalQueryProcessor import RelationalQueryProcessor
from collectionProcessor import CollectionProcessor
from TriplestoreQueryProcessor import TriplestoreQueryProcessor
from genericQueryProcessor import GenericQueryProcessor
from generic_creator import GenericQueryProcessorDeneme

rel_path = "relational.db"
met_dp = MetadataProcessor()
met_dp.setDbPathOrUrl(rel_path)
met_dp.uploadData("metadata.csv")

ann_dp = AnnotationProcessor()
ann_dp.setDbPathOrUrl(rel_path)
ann_dp.uploadData("annotations.csv")

grp_endpoint = "http://127.0.0.1:9999/blazegraph/sparql"
col_dp = CollectionProcessor()
col_dp.setDbPathOrUrl(grp_endpoint)
col_dp.uploadData("collection-1.json")
col_dp.uploadData("collection-2.json")

rel_qp = RelationalQueryProcessor()
rel_qp.setDbPathOrUrl(rel_path)

grp_qp = TriplestoreQueryProcessor()
grp_qp.setDbPathOrUrl(grp_endpoint)

# Finally, create a generic query processor for asking
# about data
generic = GenericQueryProcessor()
generic.addQueryProcessor(rel_qp)
generic.addQueryProcessor(grp_qp)

generic2 = GenericQueryProcessorDeneme()
generic2.addQueryProcessor(rel_qp)
generic2.addQueryProcessor(grp_qp)

result = generic2.getCol()
print(result)
# generic.getAllCanva()
