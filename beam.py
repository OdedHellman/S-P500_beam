import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions

INPUT_FILE = "./data/snp500source.csv"
OUTPUT_FILE = "./output/result.txt"

class SnPOptions(PipelineOptions):

  @classmethod
  def _add_argparse_args(cls, parser):
    parser.add_argument('--input', default='INPUT_FILE',
                        help='Input file')
    parser.add_argument('--output', default='OUTPUT_FILE',
                        help='Output file')
    
