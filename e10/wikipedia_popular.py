import sys
from pyspark.sql import SparkSession, functions, types

spark = SparkSession.builder.appName('reddit averages').getOrCreate()
spark.sparkContext.setLogLevel('WARN')

assert sys.version_info >= (3, 8) # make sure we have Python 3.8+
assert spark.version >= '3.2' # make sure we have Spark 3.2+


wikipedia_schema = types.StructType([
    types.StructField('language', types.StringType()),
    types.StructField('title', types.StringType()),
    types.StructField('times', types.LongType()),
    types.StructField('bytes', types.LongType()),
])

def path_to_hour_func(file_name):
    file_name = file_name.split('/')
    return file_name[-1][11:22]
    #return file_name[-15:-4]


def main(in_directory, out_directory):
    data = spark.read.csv(in_directory, sep = ' ', schema=wikipedia_schema)
    data = data.withColumn('filename', functions.input_file_name())

    path_to_hour = functions.udf(path_to_hour_func, returnType=types.StringType())
    data = data.withColumn('date', path_to_hour(data['filename']))

    data = data.filter(
        (data['language'] == 'en') &
        (data['title'] != 'Main_Page') &
        (data['title'].startswith('Special:') == False)
    )

    max = data.groupBy(data['date']).max('times')

    #wiki = data.join(max, [data['date'] == max['date'], data['times'] == max['max(times)']]).drop(max.date)
    wiki = data.join(max, 'date')
    wiki = wiki.filter(wiki['times'] == wiki['max(times)'])
    wiki = wiki.select('date', 'title', 'times')
    #wiki.show()

    wiki.sort(['date', 'title']).write.csv(out_directory + '-wiki', mode='overwrite')

if __name__=='__main__':
    in_directory = sys.argv[1]
    out_directory = sys.argv[2]
    main(in_directory, out_directory)
