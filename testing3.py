# DATAFLOW TEMPLATE TO WRITE TO BIGQUERY 
# CHANGE THE FILE TARGETS AND WHICH ONE TO USE 

import apache_beam as beam 
import os 
# WHAT IS PIPELINE OPTIONS ?? 
from apache_beam.options.pipeline_options import PipelineOptions 


pipeline_options = {
    'project': 'apache-dataflow', 
    'runner': 'DataflowRunner', 
    'region': 'southamerica-east1',
    'stage_location': 'gs://bucketforexample/temp',
    'temp_location': 'gs://bucketforexample/temp', 
    'template_location': 'gs://bucketforexample/template/batch_job_df_gcs_another_final_another',
    'save_main_session': True
}

pipeline_options = PipelineOptions.from_dictionary(pipeline_options) 
p1 = beam.Pipeline(options=pipeline_options) # Create a pipeline object 

serviceAccount = "apache-dataflow-344ca6a08675.json"
os.environ["GOOGLE_APPLICATION_CREDENTIALS"]= "apache-dataflow-344ca6a08675.json"

class split_lines(beam.DoFn):
    def process(self, element):
        return [element.split(',')]


# We will create a function that will be used inside the Pipelines 
# Now this function will be used inside the Pipeline 
class Filter(beam.DoFn): 
    def process(self, element):
        if int(element[13]) > 1900: 
            return [element]

def dict_level1(record):
    dict_ = {} 
    dict_['airport'] = record[0] 
    dict_['list'] = record[1]
    return (dict_)

def unest_dict(record):
    def expand(key, value):
        if isinstance(value, dict):
            return [(key + '_' + k, v) for k, v in unest_dict(value).items()]
        else:
            return [(key, value)]
    items = [item for k, v in record.items() for item in expand(k, v)] 
    return dict(items) 

def dict_level0(record):
    dict_ = {} 
    dict_['airport'] = record['airport']
    dict_['list_delayed_num'] = record['list_delayed_num'][0] 
    dict_['list_delayed_time'] = record['list_delayed_time'][0]
    return (dict_) 

table_schema = 'airport:STRING, list_Delayed_num:INTEGER, list_Delayed_time: INTEGER'
table = 'dataflowtesting_aggr'




pCollection = (
     p1 
     # CHANGE THIS FILE TO THE APPRIOPRIATE FILE 
     | "Import Data" >> beam.io.ReadFromText("gs://bucketforexample/property3.csv", skip_header_lines=1) 
     | "split by comma time" >> beam.ParDo(split_lines()) 
     | "Filter Delays" >> beam.ParDo(Filter())
     | "Create Key-value time" >> beam.Map(lambda record: (record[4], int(record[8])))
     | "sum by key time" >> beam.CombinePerKey(sum) 
 )
# What is the difference between CombinePerKey and combiners.Count.PerKey()
secondPcollection = (
    # CHANGE THIS FILE TO THE APPRIOPRIATE FILE 
    p1 
    | "Import Second Data" >> beam.io.ReadFromText("gs://bucketforexample/property3.csv", skip_header_lines=1)
    | "split by comma time2" >> beam.ParDo(split_lines()) 
    | "Filter Delays2" >> beam.ParDo(Filter())
    | "Create Key-value time2" >> beam.Map(lambda record: (record[4], int(record[8])))
    | "sum by key time2" >> beam.CombinePerKey(sum) 
    
)

theTable = (
    {'First': pCollection, 'Second': secondPcollection} 
    | "Group By" >> beam.CoGroupByKey() 
    | "Unest 1" >> beam.Map(lambda record: dict_level1(record))
    | "Unest 2" >> beam.Map(lambda record: unest_dict(record))
    | "Unest 3" >> beam.Map(lambda record: dict_level0(record))
    | "Write to BQ" >> beam.io.WriteToBigQuery(
        table, 
        schema=table_schema, 
        write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND, 
        create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED, 
        custom_gcs_temp_location= 'gs://bucketforexample/temp'
    )
)

p1.run() 