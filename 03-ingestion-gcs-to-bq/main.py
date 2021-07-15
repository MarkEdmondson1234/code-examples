import os
import yaml
import logging
import re
import datetime
from google.cloud import bigquery
from google.cloud.bigquery import LoadJobConfig
from google.cloud.bigquery import SchemaField
import google.cloud.logging

# set up logging https://cloud.google.com/logging/docs/setup/python
client = google.cloud.logging.Client()
client.get_default_handler()
client.setup_logging()

# load config.yaml into config
config_file = "config.yaml"

if os.path.isfile(config_file):
  with open("config.yaml", "r") as stream:
      try:
          config = yaml.safe_load(stream)
      except yaml.YAMLError as exc:
          logging.error(exc)
else:
  logging.error("config.yaml needs to be added")

# create a list of SchemaField objects from a schema config.yaml file
def create_schema(schema_config):

    SCHEMA = []
    for scheme in schema_config:

        if 'description' in scheme:
            description = scheme['description']
        else:
            description = ''

        if 'mode' in scheme:
            mode = scheme['mode']
        else:
            mode = 'NULLABLE'

        try:
            assert isinstance(scheme['name'], str)
            assert isinstance(scheme['type'], str)
            assert isinstance(mode, str)
            assert isinstance(description, str)
        except AssertionError as e:
            logging.info(
                'Error in schema: name {} - type {} - mode - {} description {}'.format(scheme['name'], scheme['type'], mode, description))
            break

        entry = SchemaField(name=scheme['name'],
                            field_type=scheme['type'],
                            mode=mode,
                            description=description)
        SCHEMA.append(entry)
        
    logging.debug('SCHEMA created {}'.format(SCHEMA))

    return SCHEMA


    
def make_tbl_name(table_id, schema=False):

    t_split = table_id.split('_20')

    name = t_split[0]
    
    if schema: return name

    suffix = ''.join(re.findall('\d\d', table_id)[0:4])

    return name + '$' + suffix
    

def query_schema(table_id, job_config):

    schema_name = make_tbl_name(table_id, schema=True)

    logging.info('Looking for schema_name: {} for import: {}'.format(schema_name, table_id))
    # if we have no configuration attempt auto-detection
    # recommended only for development tables
    if schema_name not in config['schema']:
        logging.info('No config found. Using auto detection of schema')
        job_config.autodetect = True
        return job_config

    logging.info('Found schema for ' + schema_name)

    schema_config = config['schema'][schema_name]['fields']

    job_config.schema = create_schema(schema_config)

    # standard csv load behaviour can be defined here
    job_config.quote_character = '"'
    job_config.skip_leading_rows = 1
    job_config.field_delimiter = ','
    job_config.allow_quoted_newlines = True

    return job_config

def load_gcs_bq(uri, table_id, project, dataset_id):

    client = bigquery.Client(project=project)
    dataset_ref = client.dataset(dataset_id)

    # Change the below configuration according to your import needs
    job_config = LoadJobConfig()
    job_config.source_format = bigquery.SourceFormat.CSV
    job_config.write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE
    job_config.encoding = bigquery.Encoding.UTF_8
    job_config.time_partitioning = bigquery.TimePartitioning()

    job_config = query_schema(table_id, job_config)

    table_name = make_tbl_name(table_id)
    table_ref = dataset_ref.table(table_name)

    job = client.load_table_from_uri(
        uri,
        table_ref,
        location='EU',
        job_config=job_config)  # API request



def gcs_to_bq(data, context):
    """Background Cloud Function to be triggered by Cloud Storage.
       This functions constructs the file URI and uploads it to BigQuery.

    Args:
        data (dict): The Cloud Functions event payload.
        context (google.cloud.functions.Context): Metadata of triggering event.
    Returns:
        None; the output is written to Stackdriver Logging
    """

    object_name = data['name']
    project = config['project']
    dataset_id = config['datasetid']

    if object_name:
        # create a bigquery table related to the filename
        table_id = os.path.splitext(os.path.basename(object_name))[0].replace('.','_')
        uri = 'gs://{}/{}'.format(data['bucket'], object_name)

        load_gcs_bq(uri, table_id, project, dataset_id)

    else:
        logging.info('Nothing to load')

    return