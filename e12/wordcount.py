import string, re

import sys
assert sys.version_info >= (3, 8) # make sure we have Python 3.8+
from pyspark.sql import SparkSession, functions



def main(in_directory, out_directory):
    # TODO
    wordbreak = r'[%s\s]+' % (re.escape(string.punctuation),)  # regex that matches spaces and/or punctuation
    words = spark.read.text(in_directory)
    words = words.withColumn('word', functions.explode(functions.split('value',wordbreak)))

    words = words.select(functions.lower(words['word']).alias('word'))
    #words = words.na.drop()
    words = words.filter(words['word'] != '')

    words_group = words.groupBy('word').count()
    words_group = words_group.sort(functions.desc('count'), 'word')

    #words_group.show()

    words_group.write.csv(out_directory, mode = 'overwrite')




if __name__=='__main__':
    in_directory = sys.argv[1]
    out_directory = sys.argv[2]
    spark = SparkSession.builder.appName('word count').getOrCreate()
    assert spark.version >= '3.2' # make sure we have Spark 3.2+
    spark.sparkContext.setLogLevel('WARN')

    main(in_directory, out_directory)
