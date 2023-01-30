import os
import utils
from prefect import flow
from flows.dataset_workflows.dataverse_nl_ingestion import \
    dataverse_nl_metadata_ingestion
from flows.workflow_versioning.workflow_versioner import \
    create_ingestion_workflow_versioning

TILBURG_METADATA_DIRECTORY = os.getenv('TILBURG_METADATA_DIRECTORY')
TILBURG_DATAVERSE_ALIAS = os.getenv('TILBURG_DATAVERSE_ALIAS')


@flow
def dataverse_nl_ingestion_pipeline():
    version = create_ingestion_workflow_versioning(transformer=True,
                                                   fetcher=True,
                                                   importer=True,
                                                   updater=True)

    utils.workflow_executor(dataverse_nl_metadata_ingestion,
                            TILBURG_METADATA_DIRECTORY, version,
                            TILBURG_DATAVERSE_ALIAS)


if __name__ == "__main__":
    dataverse_nl_ingestion_pipeline()
