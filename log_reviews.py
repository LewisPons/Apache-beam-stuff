import apache_beam as beam
from apache_beam.dataframe import convert
import os
import pandas as pd
from sklearn.feature_extraction.text import CountVectorizer

raw_layer_path = 'gs://raw-layer-gcp-data-eng-appr04-cee96a91'
staging_layer_path = 'gs://staging-layer-gcp-data-eng-appr04-cee96a91'

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


from apache_beam.transforms.util import Values
with beam.Pipeline() as p:
  df = pd.read_csv('movie_review.csv')  
  data_splited = (
      convert.to_pcollection(df, pipeline = p)
      | '1. To dictionary' >> beam.Map(lambda x: dict(x._asdict()))
  )
  
  cv_fit = (
      data_splited 
      |'2. Good or not' >> beam.Map(lambda x : validate_words(x, 'review_str')) 
  )    
  export = (
      p
      | '3. Singleton' >> beam.Create([None])
      | '4. As Pandas' >> beam.Map(
          lambda _, dict_iter: pd.DataFrame(dict_iter),
          dict_iter=beam.pvalue.AsIter(data_splited))
      | '5. Export' >> beam.Map(lambda x: x.to_csv('Staging/output_movie_review.csv', index=False))
  ) 
