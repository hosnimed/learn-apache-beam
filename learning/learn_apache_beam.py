import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions

import logging
import argparse

from apache_beam.runners import DirectRunner


class MyOptions(PipelineOptions):

    @classmethod
    def _add_argparse_args(cls, parser):  # type: (_BeamArgumentParser) -> None
        parser.add_argument(
            "--input-file",
            help="The input file"
        )
        parser.add_argument(
            "--output-file",
            help="The output file"
        )


def run(argv=None, saveMainSession=False):
    logging.info("____starting____")
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--input-raw-str",
        required=False,
        default="THE QUICK BROWN FOX JUMPS OVER THE LAZY DOG A B C D E F G H I J K L M N O P Q R S T U V W X Y Z"
    )
    my_pipeline_options = MyOptions(input_file="file1.txt", output_file="file2.txt")
    known_args, pipeline_args = parser.parse_known_args(argv)
    pipeline_options = PipelineOptions(pipeline_args)
    with beam.Pipeline(runner=DirectRunner(), options=pipeline_options) as p:
        logging.info("All options:\n%s", p.options.get_all_options())
        logging.info("Known Cmd Args : \n"
                     "\t -input-raw-str: %s",
                     known_args.input_raw_str)
        logging.info("MyPipelineOptions : \n"
                     "\t -input-file: %s"
                     "\t -output-file: %s",
                     my_pipeline_options.get_all_options()["input_file"],
                     my_pipeline_options.get_all_options()["output_file"]
                     )
        pass


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()
