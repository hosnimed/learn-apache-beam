import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions

import logging
import argparse

from apache_beam.runners import DirectRunner

    
class ComputeWordLengthFn(beam.DoFn):
    def process(self, element):
        # print([len(element)])
        return [len(element)]

class MyOptions(PipelineOptions):

    @classmethod
    def _add_argparse_args(cls, parser):  # type: (_BeamArgumentParser) -> None
        parser.add_argument(
            "--input-file",
            default="gs://apache-beam-sample-bucket/samples/kinglear.txt",
            help="The input file"
        )
        parser.add_argument(
            "--output-file",
            default="./target/wc/kinglear.txt",
            help="The output file"
        )


def run(argv=None, saveMainSession=False):
    logging.info("____starting____")
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--input-raw-str",
        required=False,
        default="THE QUICK BROWN FOX JUMPS OVER THE LAZY DOG"
    )
    my_pipeline_options = MyOptions(input_file="./samples/kinglear.txt")
    known_args, pipeline_args = parser.parse_known_args(argv)
    pipeline_options = PipelineOptions(pipeline_args)
    
    with beam.Pipeline(runner=DirectRunner(), options=pipeline_options) as p:
        """         
        logging.debug("All options:\n%s", p.options.get_all_options())
        logging.debug("Known Cmd Args : \n"
                        "\t -input-raw-str: %s",
                        known_args.input_raw_str)
        logging.debug("MyPipelineOptions : \n"
                        "\t -input-file: %s"
                        "\t -output-file: %s",
                        my_pipeline_options.get_all_options()["input_file"],
                        my_pipeline_options.get_all_options()["output_file"]
                        )
        
        """        
        lines = p | "ReadInputFile" >> beam.io.ReadFromText(my_pipeline_options.input_file)
        
        #ParDoFn: with DoFn
        lines_len_v1 = lines | "Mapping with ParDo Fn" >> beam.ParDo(ComputeWordLengthFn())
        print(f"lines_len_v1 : {lines_len_v1}")
        lines_len_v2 = lines | "Mapping with FlatMap Fn" >> beam.FlatMap(lambda word: [len(word)])
        print(f"lines_len_v2 : {lines_len_v2}")
        lines_len_v3 = lines | "Mapping with Map Fn" >> beam.Map(len)
        print(f"lines_len_v3 : {lines_len_v3}")


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()