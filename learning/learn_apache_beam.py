from apache_beam.options.pipeline_options import PipelineOptions
import apache_beam as beam

import logging
import argparse
import re
import os
import shutil

from apache_beam.runners import DirectRunner

class ProcessWordsMultiOutputs(beam.DoFn):
    def process(self, element, upper_bound, prefix):
        if element.lower().startswith(prefix.lower()):
            yield element 
        if len(element) <= upper_bound:
            yield beam.pvalue.TaggedOutput('Short_Words', element)
        else:
            yield beam.pvalue.TaggedOutput('Long_Words', element)

class FilterWordsUsingLength(beam.DoFn):
    def process(self,element,lower_bound,upper_bound):
        if lower_bound <= len(element) <= upper_bound:
            yield element

class CombineAllMarks(beam.CombineFn):

    def __init__(self, is_per_key=False):
        self.is_per_key = is_per_key

    def create_accumulator(self):
        return (0.0, 0)

    def add_input(self, acc, elem):
        (sum, count) = acc
        if self.is_per_key:
            return sum + elem[1], count + 1
        else:
            return sum + elem[2], count + 1

    def merge_accumulators(self, accumulators):
        sums, counts = zip(*accumulators)
        return sum(sums), sum(counts)

    def extract_output(self, sum_count):
        (sum, count) = sum_count
        return sum / count if count > 0 else 0.0

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
            (lines
            | "Extract" >> beam.ParDo(ExtractWordsFn())
            | "Lower" >> beam.ParDo(lambda w: w.lower())
            | "PairWithOne" >> beam.Map(lambda w: (w, 1))
            | "GrouByKey" >> beam.GroupByKey()
            | "Count" >> beam.CombineValues(sum)
            | "WriteToFile" >> beam.io.WriteToText(os.getcwd()+"/target/word_count.out.txt")
            )

            #CoGroupByKey
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
    
            joined_result = ( {"emails":emails, "phones":phones} | beam.CoGroupByKey())

            def join_person_info(person_infos):
                name, info = person_infos
                emails, phones = info["emails"], info["phones"]
                return f"{name} : {emails} - {phones}"

            (joined_result 
            | "Show person info" >> beam.Map(join_person_info)
            | "Write infos to file" >> beam.io.WriteToText(os.getcwd()+"/target/person_info.txt")
            )  

            #CombineGlobally
            student_subjects_marks = [
            ("Joseph", "Maths", 83), ("Joseph", "Physics", 74), ("Joseph", "Chemistry", 91), 
            ("Joseph", "Biology", 82), ("Jimmy", "Maths", 69), ("Jimmy", "Physics", 62), 
            ("Jimmy", "Chemistry", 97), ("Jimmy", "Biology", 80), ("Tina", "Maths", 78), 
            ("Tina", "Physics", 73), ("Tina", "Chemistry", 68), ("Tina", "Biology", 87), 
            ("Thomas", "Maths", 87), ("Thomas", "Physics", 93), ("Thomas", "Chemistry", 91), 
            ("Thomas", "Biology", 74), ("Cory", "Maths", 56), ("Cory", "Physics", 65), 
            ("Cory", "Chemistry", 71), ("Cory", "Biology", 68), ("Jackeline", "Maths", 86), 
            ("Jackeline", "Physics", 62), ("Jackeline", "Chemistry", 75), ("Jackeline", "Biology", 83), 
            ("Juan", "Maths", 63), ("Juan", "Physics", 69), ("Juan", "Chemistry", 64),("Juan", "Biology", 60)] 
    
            def print_row(row, *args):
                print("="*100)
                for v in args:
                    print(v)
                print(row)
                print("="*100)

            students_results = p | "CreateStudentResult" >> beam.Create(student_subjects_marks)
           
            (students_results 
            | beam.CombineGlobally(CombineAllMarks()).with_defaults() # return empty PCollection if input is empty
            | "Show Result" >> beam.Map(print_row,"GlobalAverage") )

            #CombinePerKey
            (students_results
            | "Group per name" >> beam.Map(lambda tuple: (tuple[0], (tuple[1], tuple[2])))
            | "Compute avg per student" >> beam.CombinePerKey(CombineAllMarks(is_per_key=True)) 
            | "Show Result Per Key" >> beam.Map(print_row,"AveragePerStudent")
            # | "Write avg marks to file" >> beam.io.WriteToText(os.getcwd()+"/target/avg_mark_per_student.txt")
            )

            #Flatten
            joseph_subjects_marks = p | "Create Joseph PCol" >> beam.Create(student_subjects_marks[:3])
            juan_subjects_marks   = p | "Create Juan PCol" >> beam.Create(student_subjects_marks[-4:])
            ((joseph_subjects_marks, juan_subjects_marks)
            | beam.Flatten()
            | "Write Flattened to File" >> beam.io.WriteToText(os.getcwd()+"/target/joseph_and_juan.txt")
            )
            
            #Partition
            def partition_fn(student, num_partitions):
                (_, subject, _) = student
                subjects  = 'Maths','Physics','Chemistry','Biology', 
                return subjects.index(subject)

            all_partitions = student_subjects_marks | beam.Partition(partition_fn, 4)
            (all_partitions['0'] 
            # | "Show Maths students" >> beam.Map(print_row, "Math Student") )
            | "Write Maths students to File" >> beam.io.WriteToText(os.getcwd()+"/target/maths_students.txt") )
            
            #SideInput
            (
                lines | "SideInput : Extract words" >> beam.ParDo(ExtractWordsFn())
                | "Filter using length" >> beam.ParDo(FilterWordsUsingLength(),lower_bound=2,upper_bound=5)
                | "Write small words" >> beam.io.WriteToText(os.getcwd()+"/target/small_words.txt")
            )

           #SideOutput
            prefix = 'O'
            outputs = (
                lines | "SideOutput : Extract words" >> beam.ParDo(ExtractWordsFn())
                | "SideOutput :  Filter using length" >> beam.ParDo(ProcessWordsMultiOutputs(),upper_bound=5, prefix=prefix)
                .with_outputs('Short_Words', 'Long_Words', main='Start_With')
            )
            short_words = outputs.Short_Words
            long_words  = outputs.Long_Words
            start_with  = outputs.Start_With
            short_words | "SideOutput: Write short words" >> beam.io.WriteToText(os.getcwd()+"/target/side_output/short_words.txt")
            long_words  | "SideOutput : Write long words" >> beam.io.WriteToText(os.getcwd()+"/target/side_output/long_words.txt")
            start_with  | "SideOutput : Write words : start with" >> beam.io.WriteToText(os.getcwd()+f"/target/side_output/start_with_{prefix}.txt")

if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()