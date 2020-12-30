import logging
import os

import apache_beam as beam
import pandas as pd
from apache_beam.options.pipeline_options import (GoogleCloudOptions,
                                                  PipelineOptions,
                                                  SetupOptions,
                                                  StandardOptions)

from bigquery.table import SCHEMA, TABLE_ID


class MyOptions(PipelineOptions):
    @classmethod
    def _add_argparse_args(cls, parser):
        parser.add_argument('--input', help='Path of the file to read from')
        parser.add_argument('--dataset', help='GCP BigQuery dataset')

class ReadData(beam.PTransform):
    def __init__(self, filepath: str):
        super().__init__()
        if filepath == None or filepath == '':
            raise ValueError()

        self.beers = os.path.join(filepath, 'beers.csv')
        self.breweries = os.path.join(filepath, 'breweries.csv')
        logging.info(f'beers: {self.beers}')
        logging.info(f'beers: {self.breweries}')

    def expand(self, p):
        def read_data(file: str) -> pd.DataFrame:
            df = pd.read_csv(file, dtype=str)
            df = df.where(pd.notnull(df), None)
            return df

        df_beers = read_data(self.beers)
        df_beers = df_beers.drop(['row_id'], axis=1)

        df_breweries = read_data(self.breweries)
        df_breweries = df_breweries.rename({
            'id': 'brewery_id',
            'name': 'brewery_name',
            'city': 'brewery_city',
            'state': 'brewery_state'
        }, axis=1)

        df = df_beers.merge(right=df_breweries, on='brewery_id')

        return (
            p
            | 'Create' >> beam.Create(df.to_dict('records'))
        )

class CleanData(beam.DoFn):
    def process(self, element):
        if element['id'] != None and \
            element['name'] != None and \
            element['style'] != None and \
            element['abv'] != None:
            yield element

class ConvertType(beam.DoFn):
    def process(self, element):
        element['abv'] = float(element['abv']) if element['abv'] != None else None
        element['ibu'] = int(float(element['ibu'])) if element['ibu'] != None else None
        element['ounces'] = float(element['ounces']) if element['ounces'] != None else None
        yield element

def run():
    pipeline_options = PipelineOptions()

    my_options = pipeline_options.view_as(MyOptions)
    logging.info(f'my_options: {my_options}')
    google_cloud_options = pipeline_options.view_as(GoogleCloudOptions)
    logging.info(f'google_cloud_options: {google_cloud_options}')
    standard_options = pipeline_options.view_as(StandardOptions)
    logging.info(f'standard_options: {standard_options}')
    setup_options = pipeline_options.view_as(SetupOptions)
    logging.info(f'setup_options: {setup_options}')

    with beam.Pipeline(options=pipeline_options) as pipeline:
        (
            pipeline
            | 'Read Data' >> ReadData(my_options.input)
            | 'Clean Data' >> beam.ParDo(CleanData())
            | 'Convert Data Type' >> beam.ParDo(ConvertType())
            | 'To BigQuery' >> beam.io.WriteToBigQuery(
                TABLE_ID,
                dataset=my_options.dataset,
                project=google_cloud_options.project,
                schema=SCHEMA,
                create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE,
                kms_key=google_cloud_options.dataflow_kms_key,
                method='FILE_LOADS'
            )
        )

if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()
