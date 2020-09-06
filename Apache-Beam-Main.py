# -*- coding: utf-8 -*-
"""
Created on Sun Sep  6 13:38:33 2020

@author: andyl
"""

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
      default='testA.csv',
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
    csv_lines = (
            p | beam.io.ReadFromText(known_args.input) 
              | beam.ParDo(Split())
              | beam.Map(lambda x: ((x['Street'],x['Postcode'],x['PAON'],x['SAON']),(x['TransactionID'],x['Date'],x['Price'])))
              | beam.GroupByKey()
              | beam.io.WriteToText(known_args.output)
                 )