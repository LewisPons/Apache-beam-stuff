import apache_beam as beam
from apache_beam.dataframe import convert
import pandas as pd
from sklearn.feature_extraction.text import CountVectorizer


def validate_words(row, key):
  cv = CountVectorizer()
  cv_fit = cv.fit_transform( [row.get(key)] )
  words = cv.get_feature_names_out()
  check =  any(item in ['good', 'GOOD', 'goOd', 'gOod', 'gOOD', 'Good'] for item in words.tolist())
  if check is True:
    row['positive_review'] = 1
    return row
  else :
    row['positive_review'] = 0
    return row


from csv import DictWriter
from csv import excel
# from cStringIO import StringIO
from io import StringIO 
def _dict_to_csv(element, column_order, missing_val='', discard_extras=True, dialect=excel):
    """ Additional properties for delimiters, escape chars, etc via an instance of csv.Dialect
        Note: This implementation does not support unicode
    """
    buf = StringIO()
    writer = DictWriter(buf,
                        fieldnames=column_order,
                        restval=missing_val,
                        extrasaction=('ignore' if discard_extras else 'raise'),
                        dialect=dialect)
    writer.writerow(element)

    return buf.getvalue().rstrip(dialect.lineterminator)

class _DictToCSVFn(beam.DoFn):
    """ Converts a Dictionary to a CSV-formatted String
        column_order: A tuple or list specifying the name of fields to be formatted as csv, in order
        missing_val: The value to be written when a named field from `column_order` is not found in the input element
        discard_extras: (bool) Behavior when additional fields are found in the dictionary input element
        dialect: Delimiters, escape-characters, etc can be controlled by providing an instance of csv.Dialect

    """
    def __init__(self, column_order, missing_val='', discard_extras=True, dialect=excel):
        self._column_order = column_order
        self._missing_val = missing_val
        self._discard_extras = discard_extras
        self._dialect = dialect

    def process(self, element, *args, **kwargs):
        result = _dict_to_csv(element,
                              column_order=self._column_order,
                              missing_val=self._missing_val,
                              discard_extras=self._discard_extras,
                              dialect=self._dialect)
        return [result,]

class DictToCSV(beam.PTransform):
    """ Transforms a PCollection of Dictionaries to a PCollection of CSV-formatted Strings
        column_order: A tuple or list specifying the name of fields to be formatted as csv, in order
        missing_val: The value to be written when a named field from `column_order` is not found in an input element
        discard_extras: (bool) Behavior when additional fields are found in the dictionary input element
        dialect: Delimiters, escape-characters, etc can be controlled by providing an instance of csv.Dialect
    """
    def __init__(self, column_order, missing_val='', discard_extras=True, dialect=excel):
        self._column_order = column_order
        self._missing_val = missing_val
        self._discard_extras = discard_extras
        self._dialect = dialect

    def expand(self, pcoll):
        return pcoll | beam.ParDo(_DictToCSVFn(column_order=self._column_order,
                                          missing_val=self._missing_val,
                                          discard_extras=self._discard_extras,
                                          dialect=self._dialect)
                             )
    
with beam.Pipeline() as p:
  df = pd.read_csv('/content/litle_movie_review.csv')  
  # raw_csv = (p 
  #           #  | '1. read csv' >> beam.io.ReadFromText('/content/movie_review.csv')
  #            | beam.dataframe.io.read_csv('/content/movie_review.csv', usecols = ['cid', 'review_str' , 'id_review'])
  #           #  | beam.Map(lambda x: x.split(',') )

  data_splited = (
      convert.to_pcollection(df, pipeline = p)
      | beam.Map(lambda x: dict(x._asdict()))
  )
  
  cv_fit = (
      data_splited 
      | beam.Map(lambda x : validate_words(x, 'review_str')) 
      # | beam.Map(lambda x: x[0]) 
      | beam.Map(print)
  )    
