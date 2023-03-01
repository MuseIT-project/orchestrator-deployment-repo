import os
import re

from configuration.config import settings


def retrieve_license_name(license_string):
    dataset_lic = ''
    if re.search(r'creativecommons', license_string):
        if re.search(r'/by/4\.0', license_string):
            dataset_lic = "CC BY 4.0"
        elif re.search(r'/by-nc/4\.0', license_string):
            dataset_lic = "CC BY-NC 4.0"
        elif re.search(r'/by-sa/4\.0', license_string):
            dataset_lic = "CC BY-SA 4.0"
        elif re.search(r'/by-nc-sa/4\.0', license_string):
            dataset_lic = "CC BY-NC-SA 4.0"
        elif re.search(r'zero/1\.0', license_string):
            dataset_lic = "CC0 1.0"
    elif re.search(r'DANSLicence', license_string):
        dataset_lic = "DANS Licence"
    return dataset_lic


def is_lower_level_liss_study(metadata):
    title = metadata['datasetVersion']['metadataBlocks']['citation'][
        'fields'][0]['value']
    print("Title is", title)
    square_bracket_amount = title.count('>')
    if square_bracket_amount == 0:
        print('no square brackets')
        return False, title
    if square_bracket_amount == 1:
        liss_match = re.search(r'L[iI]SS [Pp]anel', title)
        immigrant_match = re.search(r'Immigrant [Pp]anel', title)
        if liss_match or immigrant_match:
            if liss_match:
                print("Matched on liss panel")
                return False, title
            if immigrant_match:
                print("Matched on immigrant panel")
                return False, title
        else:
            return True, title
    if square_bracket_amount >= 2:
        return True, title


def workflow_executor(
        data_provider_workflow,
        version,
        settings_dict_name
):
    """
    Executes the workflow of a give data provider for each metadata file.

    Takes workflow flow that ingests a single metadata file of a data provider
    and executes that workflow for every metadata file in the given directory.

    For Dataverse to Dataverse ingestion, the url and api key of the source
    Dataverse are required.

    :param data_provider_workflow: The workflow to ingest the metadata file.
    :param version: A dictionary containing all version info of the workflow.
    :param settings_dict_name: string, name of the settings you wish to use
    :return: None
    """
    settings_dict = getattr(settings, settings_dict_name)
    metadata_directory = settings_dict.METADATA_DIRECTORY

    files = [f for f in os.listdir(metadata_directory) if
             not f.startswith('.')]
    for filename in files:
        file_path = os.path.join(metadata_directory, filename)
        if os.path.isfile(file_path):
            data_provider_workflow(
                file_path,
                version,
                settings_dict,
                return_state=True
            )

        break
