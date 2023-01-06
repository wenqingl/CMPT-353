import sys
assert sys.version_info >= (3, 8) # make sure we have Python 3.8+
from pyspark.sql import SparkSession, functions, types

comments_schema = types.StructType([
    types.StructField('archived', types.BooleanType()),
    types.StructField('author', types.StringType()),
    types.StructField('author_flair_css_class', types.StringType()),
    types.StructField('author_flair_text', types.StringType()),
    types.StructField('body', types.StringType()),
    types.StructField('controversiality', types.LongType()),
    types.StructField('created_utc', types.StringType()),
    types.StructField('distinguished', types.StringType()),
    types.StructField('downs', types.LongType()),
    types.StructField('edited', types.StringType()),
    types.StructField('gilded', types.LongType()),
    types.StructField('id', types.StringType()),
    types.StructField('link_id', types.StringType()),
    types.StructField('name', types.StringType()),
    types.StructField('parent_id', types.StringType()),
    types.StructField('retrieved_on', types.LongType()),
    types.StructField('score', types.LongType()),
    types.StructField('score_hidden', types.BooleanType()),
    types.StructField('subreddit', types.StringType()),
    types.StructField('subreddit_id', types.StringType()),
    types.StructField('ups', types.LongType()),
    #types.StructField('year', types.IntegerType()),
    #types.StructField('month', types.IntegerType()),
])


def main(in_directory, out_directory):
    # TODO

    comments = spark.read.json(in_directory, schema=comments_schema)
    comments = comments.select(comments['subreddit'], comments['score'], comments['author'],)
    averages = comments.groupBy('subreddit').avg('score')
    averages = averages.filter(averages['avg(score)'] > 0)

    comments_avg = comments.join(averages.hint('broadcast'),'subreddit', ).cache()
    #comments_avg = comments.join(averages,'subreddit', ).cache()
    comments_avg = comments_avg.withColumn('rel_score', comments_avg['score'] / comments_avg['avg(score)'])

    max = comments_avg.groupBy('subreddit').max('rel_score')

    comments_avg_max = comments_avg.join(max, 'subreddit')
    comments_avg_max = comments_avg_max.filter(comments_avg_max['rel_score'] == comments_avg_max['max(rel_score)'])
    
    best_author = comments_avg_max.select(
        comments_avg_max['subreddit'],
        comments_avg_max['author'],
        comments_avg_max['rel_score'],
    )
    #comments_avg_max.show()

    best_author.write.json(out_directory, mode='overwrite')


if __name__=='__main__':
    in_directory = sys.argv[1]
    out_directory = sys.argv[2]
    spark = SparkSession.builder.appName('Reddit Relative Scores').getOrCreate()
    assert spark.version >= '3.2' # make sure we have Spark 3.2+
    spark.sparkContext.setLogLevel('WARN')

    main(in_directory, out_directory)
