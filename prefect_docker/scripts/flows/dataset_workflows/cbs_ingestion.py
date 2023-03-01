from configuration.config import settings
from prefect import flow
from prefect.orion.schemas.states import Completed, Failed
from tasks.base_tasks import xml2json, dataverse_mapper, \
    dataverse_import, update_publication_date, add_workflow_versioning_url


@flow
def cbs_metadata_ingestion(file_path, alias, version):
    json_metadata = xml2json(file_path)
    if not json_metadata:
        return Failed(message='Unable to transform from xml to json.')

    mapped_metadata = dataverse_mapper(
        json_metadata,
        settings.CBS_MAPPING_FILE_PATH,
        settings.CBS_TEMPLATE_FILE_PATH,
        False
    )

    if not mapped_metadata:
        return Failed(message='Unable to map metadata.')

    mapped_metadata = add_workflow_versioning_url(mapped_metadata, version)
    if not mapped_metadata:
        return Failed(message='Unable to store workflow version.')

    fields = mapped_metadata['datasetVersion']['metadataBlocks']['citation'][
        'fields']
    split_path = file_path.split('/')[3]
    doi = 'doi:10.57934/' + split_path.split('_')[0]
    import_response = dataverse_import(mapped_metadata, alias,
                                       doi)
    if not import_response:
        return Failed(message='Unable to import dataset into Dataverse.')

    publication_date = next((field for field in fields if
                             field.get('typeName') == 'distributionDate'),
                            {})
    if publication_date["value"]:
        pub_date_response = update_publication_date(publication_date["value"],
                                                    doi)
        if not pub_date_response:
            return Failed(message='Unable to update publication date.')
    return Completed(message=file_path + 'ingested successfully.')
