import argparse
import json
import logging

import apache_beam as beam
from apache_beam.metrics.metric import Metrics
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
from apache_beam.options.pipeline_options import StandardOptions
from apache_beam.transforms import trigger

class ParseEventFn(beam.DoFn):
  """Parses the raw event info into a Python dictionary.
  """
  def __init__(self):
    beam.DoFn.__init__(self)
    self.num_parse_errors = Metrics.counter(self.__class__, 'num_parse_errors')

  def process(self, elem):
    try:
      raw = json.loads(elem)
      yield raw
    except:
      self.num_parse_errors.inc()
      logging.error('Parse error on "%s"', elem)

class AssignGroupingKey(beam.DoFn):
    """Assign grouping key to events
    """
    def __init__(self, grouping_keys: list[str]):
       self.grouping_keys = grouping_keys

    def process(self, element):
        grouping_key = '.'.join([ str(element[key]) for key in self.grouping_keys ])
        yield (grouping_key, 1)

class CountEvents(beam.PTransform):
  """Count events grouped by a key.
  """
  def __init__(self, grouping_keys: list[str], window_duration: int, allowed_lateness: int):
    beam.PTransform.__init__(self)
    self.grouping_keys = grouping_keys
    self.window_duration_seconds = window_duration
    self.allowed_lateness_seconds = allowed_lateness

  def expand(self, pcoll):
    return (
        pcoll
        | 'Windowing' >> beam.WindowInto(
            beam.window.FixedWindows(self.window_duration_seconds),
            allowed_lateness=self.allowed_lateness_seconds,
            trigger=trigger.AfterWatermark(
                late=trigger.AfterProcessingTime(20)),
            accumulation_mode=trigger.AccumulationMode.DISCARDING)
        | 'AssignGroupingKey' >> beam.ParDo(AssignGroupingKey(self.grouping_keys))
        | 'SumByKey' >> beam.CombinePerKey(sum))

def run(argv=None, save_main_session=True):
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--subscription',
        type=str,
        default='projects/local-project/subscriptions/impression_event_testing',
        help='Pub/Sub subscription to read from')
    parser.add_argument(
        '--window_duration',
        type=int,
        default=5,
        help='Numeric value of fixed window duration for aggregation '
        ', in seconds')
    parser.add_argument(
        '--allowed_lateness',
        type=int,
        default=120,
        help='Numeric value of allowed data lateness, in seconds')
    parser.add_argument(
        '--grouping_keys',
        type=str,
        required=True,
        nargs='+',
        help='Grouping keys. You can specify more than one')
    
    args, pipeline_args = parser.parse_known_args(argv)

    options = PipelineOptions(pipeline_args)

    options.view_as(SetupOptions).save_main_session = save_main_session
    options.view_as(StandardOptions).streaming = True

    with beam.Pipeline(options=options) as p:
        messages = p | 'ReadPubSub' >> beam.io.ReadFromPubSub(
            subscription=args.subscription)
        
        events = (
            messages
            | 'ParseEventFn' >> beam.ParDo(ParseEventFn())
            | 'CountEvents' >> CountEvents(args.grouping_keys, args.window_duration, args.allowed_lateness)
            | 'Print' >> beam.Map(lambda elem: print(elem))) # TODO(dana): write to database

if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  run()