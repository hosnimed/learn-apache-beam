import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions

import logging
import argparse
import re
import os
import shutil

from apache_beam.runners import DirectRunner

    
class ExtractWordsFn(beam.DoFn):
    def process(self, element):
        return re.findall(r'[\w]+', element)


class ComputeWordLengthFn(beam.DoFn):
    def process(self, element):
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
    
    try:
        shutil.rmtree(os.getcwd()+"/target", ignore_errors=True)
    except OSError as error:
        logging.error(error)
    else:
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
            
            """     
            #ParDoFn: with DoFn
            lines_len_v1 = lines | "Mapping with ParDo Fn" >> beam.ParDo(ComputeWordLengthFn()) | "Write lines_len_v1" >> beam.io.WriteToText(os.getcwd()+"/target/lines_len_v1.out.txt")
            lines_len_v2 = lines | "Mapping with FlatMap Fn" >> beam.FlatMap(lambda word: [len(word)]) | "Write lines_len_v2" >> beam.io.WriteToText(os.getcwd()+"/target/lines_len_v2.out.txt")
            lines_len_v3 = lines | "Mapping with Map Fn" >> beam.Map(len) | "Write lines_len_v3" >> beam.io.WriteToText(os.getcwd()+"/target/lines_len_v3.out.txt")
        
            #Filter
            non_empty_lines = lines | "Filter empty lines" >> beam.Filter(lambda x: len(x) > 0) | "Write non empty lines" >> beam.io.WriteToText(os.getcwd()+"/target/non_empty_lines.out.txt") 
            """

            #GroupByKey
            words_count = (lines
            | "Extract" >> beam.ParDo(ExtractWordsFn())
            | "Lower" >> beam.ParDo(lambda w: w.lower())
            | "PairWithOne" >> beam.Map(lambda w: (w, 1))
            | "GrouByKey" >> beam.GroupByKey()
            | "Count" >> beam.CombineValues(sum)
            | "WriteToFile" >> beam.io.WriteToText(os.getcwd()+"/target/word_count.out.txt")
            )

            #CoGroupKey
            emails_list = [
            ('amy', 'amy@example.com'),
            ('carl', 'carl@example.com'),
            ('julia', 'julia@example.com'),
            ('carl', 'carl@email.com'),
            ]
            phones_list = [
                ('amy', '111-222-3333'),
                ('james', '222-333-4444'),
                ('amy', '333-444-5555'),
                ('carl', '444-555-6666'),
            ]

            emails = p | 'CreateEmails' >> beam.Create(emails_list)
            phones = p | 'CreatePhones' >> beam.Create(phones_list)
    
            joined_result = ( {"emails":emails_list, "phones":phones_list} | beam.CoGroupByKey())

            def join_person_info(person_infos):
                name, info = person_infos
                emails, phones = info["emails"], info["phones"]
                return f"{name} : {emails} - {phones}"

            persons_infos = (joined_result 
            | "Show person info" >> beam.Map(join_person_info)
            | "Write infos to file" >> beam.io.WriteToText(os.getcwd()+"/target/person_info.txt")
            )  

if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()