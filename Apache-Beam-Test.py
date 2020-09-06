# -*- coding: utf-8 -*-
import argparse
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions


parser = argparse.ArgumentParser()
parser.add_argument(
      '--input',
      dest='input',
      default='pp-monthly-update-new-version.csv',
      help='Input file to process.')
parser.add_argument(
      '--output',
      dest='output',
      default='test.csv',
      help='Output file to write results to.')
known_args, pipeline_args = parser.parse_known_args()


class Split(beam.DoFn):
    def process(self, element):
        if len(element.split(",")) == 16:
            TransID,Price,Date,Postcode,PropertyType,OldNew,Duration,PAON,SAON,Street,Locality,TownCity,District,County,PPDCat,RecordStatus = element.split(",") 
            return [{
                'TransactionID': TransID,
                'Date': Date,
                'Postcode': Postcode,
                'Street': Street,
                'Locality': Locality,
                'Town/City': TownCity,
                'District': District,
                'County': County,
                'PAON': PAON,
                'SAON': SAON,
                'Price': Price
                }]
    
pipeline_options = PipelineOptions(pipeline_args)

with beam.Pipeline(options=pipeline_options) as p:
  TestForProperty1 = (
            p | beam.io.ReadFromText(known_args.input) 
              | beam.ParDo(Split())
              | beam.Filter(lambda x: x['Street'] == '"RAWLEY LANE"')
              | beam.Filter(lambda x: x['SAON'] == '"FLAT 3"')
              | beam.Map(lambda x: ((x['Street'],x['Postcode'],x['PAON'],x['SAON']),(x['TransactionID'],x['Date'],x['Price'])))
              | beam.combiners.Count.PerKey()
              # The particular property of interest has been sold 3 times
              | beam.Map(print))
#              | beam.io.WriteToText(known_args.output))

# with beam.Pipeline(options=pipeline_options) as p:
#   TestForProperty2 = (
#             p | beam.io.ReadFromText(known_args.input) 
#               | beam.ParDo(Split())
#               | beam.Filter(lambda x: x['Street'] == '"RAWLEY LANE"')
#               | beam.Filter(lambda x: x['SAON'] == '"FLAT 3"')
#               | beam.Map(lambda x: ((x['Street'],x['Postcode'],x['PAON'],x['SAON']),x['Price']))
#               | beam.CombinePerKey(sum)
#               # The particular property of interest has been sold 3 times for a total of £417,500
#               | beam.Map(print))