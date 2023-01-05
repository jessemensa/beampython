# PARDO => DO SOMETHING IN PARALLEL 
# THIS RUNS THE FILE LOCALLY AND INTO CLOUD STORAGE 
import apache_beam as beam 
import os 

serviceAccount = "apache-dataflow-344ca6a08675.json"
os.environ["GOOGLE_APPLICATION_CREDENTIALS"]= "/Users/jessmensa/Desktop/Beamgcp/apache-dataflow-344ca6a08675.json"

p1 = beam.Pipeline() # Create a pipeline objecy 

# We will create a function that will be used inside the Pipelines 
# Now this function will be used inside the Pipeline 
class Filter(beam.DoFn): 
    def process(self, element):
        if int(element[13]) > 1900: 
            return [element]

pCollection = (
     p1 
     | "Import Data" >> beam.io.ReadFromText("property3.csv", skip_header_lines=1) 
     | "Split by commas" >> beam.Map(lambda record: record.split(',')) 
     | "Filter Delays" >> beam.ParDo(Filter()) # Use the ParDo Transform 
     | "Create a key value pair" >> beam.Map(lambda record: (record[3], int(record[13])))
     | "Sum by Key" >> beam.CombinePerKey(sum)
 )
# What is the difference between CombinePerKey and combiners.Count.PerKey()
secondPcollection = (
    p1 
    | "Import Second Data" >> beam.io.ReadFromText("property3.csv", skip_header_lines=1)
    | "Split by commas 2" >> beam.Map(lambda record: record.split(',')) 
    | "Filter Delays 2" >> beam.ParDo(Filter()) 
    | "Create a key value pair 2" >> beam.Map(lambda record: (record[3], int(record[13])))
    | "Count by Key2" >> beam.combiners.Count.PerKey() 
)

theTable = (
    {'First': pCollection, 'Second': secondPcollection} 
    | beam.CoGroupByKey() 
    | "Save to Google Cloud Storage" >> beam.io.WriteToText("gs://bucketforexample/results.csv")
)

p1.run() 
