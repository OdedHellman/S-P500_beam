import logging
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions

INPUT_FILE = "./data/stock_data.csv"
OUTPUT_FILE = "./output/result.txt"
HEADER = 'Date,Open,High,Low,Close,Volume'

class StocksOptions(PipelineOptions):
    """ Pipeline options for the stocks pipeline """
    @classmethod
    def _add_argparse_args(cls, parser):
        parser.add_argument('--input', default=INPUT_FILE,
                            help='Input file')
        parser.add_argument('--output', default=OUTPUT_FILE,
                            help='Output file')

class Split(beam.DoFn):
    """ Split the data using ',' as a delimiter """
    
    @staticmethod
    def process(element):
        date, open, _, _, close, _ = element.split(',')
        return [{
            'date': date,
            'open': float(open),
            'close': float(close)
        }]

class GroupByOpen(beam.DoFn):
    """ Group the data by the open value """
    
    @staticmethod
    def process(element):
        return [('Open', element['open'])]

class GroupByClose(beam.DoFn):
    """ Group the data by the close value """
    
    @staticmethod
    def process(element):
        return [('Close', element['close'])]

def main():
    options = StocksOptions()
    
    with beam.Pipeline(options=options) as pipeline:
        source_lines = (pipeline |
        'ReadFile' >> beam.io.ReadFromText(options.input, skip_header_lines=1) |
        'Split' >> beam.ParDo(Split())
        )
        
        # Claculate the mean of the open values
        mean_open = (source_lines | beam.ParDo(GroupByOpen()) |
                     'GroupByOpen' >> beam.GroupByKey() |
                     'MeanOpen' >> beam.CombineValues(beam.combiners.MeanCombineFn())
                     )
        
        # Claculate the mean of the close values
        mean_close = (source_lines | beam.ParDo(GroupByClose()) |
                      'GroupByClose' >> beam.GroupByKey() |
                      'MeanClose' >> beam.CombineValues(beam.combiners.MeanCombineFn())
                      )
        
        # Write the results to a file
        (mean_open, mean_close) | beam.Flatten() | beam.io.WriteToText(options.output)

if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    main()
