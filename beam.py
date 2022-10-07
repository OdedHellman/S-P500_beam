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
        parser.add_argument('--input', default='INPUT_FILE',
                            help='Input file')
        parser.add_argument('--output', default='OUTPUT_FILE',
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
        return ['Open', (element['open'])]

class GroupByClose(beam.DoFn):
    """ Group the data by the close value """
    
    @staticmethod
    def process(element):
        return ['Close', (element['close'])]

def main():

if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    main()
