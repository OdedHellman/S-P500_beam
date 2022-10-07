import logging
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions

INPUT_FILE = "./data/snp500source.csv"
OUTPUT_FILE = "./output/result.txt"
HEADER = 'Date,Open,High,Low,Close,Volume'
class SnPOptions(PipelineOptions):

  @classmethod
  def _add_argparse_args(cls, parser):
    parser.add_argument('--input', default='INPUT_FILE',
                        help='Input file')
    parser.add_argument('--output', default='OUTPUT_FILE',
                        help='Output file')

class Split(beam.DoFn):
    def process(self, element):
        date, open, _, _, close, _ = element.split(',')
        return [{
            'date': date,
            'open': float(open),
            'close': float(close)
        }]
