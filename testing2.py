# MOVE FILE FROM CLOUD STORAGE TO CLOUD STORAGE 
# HAVE TO CREATE FOUR FOLDERS IN CLOUD STORAGE 
#  TEMP FOLDER, TEMPLATE FOLDER, OUTPUT FOLDER
# 
# THIS CREATED A DATAFLOW TEMPLATE  
# THEN NEXT WE HAVE TO CREATE THE JOB ITSELF 

# THIS DIDNT WORK, DELETE THE TEMP FILES AND TEMPORARY FOLDERS AND RE DO IT AGAIN ON WEEKEND 
import apache_beam as beam 
import os 
# WHAT IS PIPELINE OPTIONS ?? 
# 
from apache_beam.options.pipeline_options import PipelineOptions 


pipeline_options = {
    'project': 'apache-dataflow', 
    'runner': 'DataflowRunner', 
    'region': 'southamerica-east1',
    'stage_location': 'gs://bucketforexample/temp',
    'temp_location': 'gs://bucketforexample/temp', 
    'template_location': 'gs://bucketforexample/template/batch_job_df_gcs_another_final_another' 
}

pipeline_options = PipelineOptions.from_dictionary(pipeline_options) 
p1 = beam.Pipeline(options=pipeline_options) # Create a pipeline object 

serviceAccount = "apache-dataflow-344ca6a08675.json"
os.environ["GOOGLE_APPLICATION_CREDENTIALS"]= "apache-dataflow-344ca6a08675.json"


# We will create a function that will be used inside the Pipelines 
# Now this function will be used inside the Pipeline 
class Filter(beam.DoFn): 
    def process(self, element):
        if int(element[13]) > 1900: 
            return [element]

pCollection = (
     p1 
     | "Import Data" >> beam.io.ReadFromText("gs://bucketforexample/property3.csv", skip_header_lines=1) 
     | "Split by commas" >> beam.Map(lambda record: record.split(',')) 
     | "Filter Delays" >> beam.ParDo(Filter()) # Use the ParDo Transform 
     | "Create a key value pair" >> beam.Map(lambda record: (record[3], int(record[13])))
     | "Sum by Key" >> beam.CombinePerKey(sum)
 )
# What is the difference between CombinePerKey and combiners.Count.PerKey()
secondPcollection = (
    p1 
    | "Import Second Data" >> beam.io.ReadFromText("gs://bucketforexample/property3.csv", skip_header_lines=1)
    | "Split by commas 2" >> beam.Map(lambda record: record.split(',')) 
    | "Filter Delays 2" >> beam.ParDo(Filter()) 
    | "Create a key value pair 2" >> beam.Map(lambda record: (record[3], int(record[13])))
    | "Count by Key2" >> beam.combiners.Count.PerKey() 
)

theTable = (
    {'First': pCollection, 'Second': secondPcollection} 
    | beam.CoGroupByKey() 
    | "Save to Google Cloud Storage" >> beam.io.WriteToText("gs://bucketforexample/output/secondresultsfinal.csv")
)

p1.run() 