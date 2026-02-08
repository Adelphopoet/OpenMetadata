from metadata.ingestion.source.dashboard.datalens.metadata import DataLensSource
from metadata.utils.service_spec import BaseSpec

ServiceSpec = BaseSpec(metadata_source_class=DataLensSource)
