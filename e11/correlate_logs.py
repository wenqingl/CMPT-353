import sys
assert sys.version_info >= (3, 8) # make sure we have Python 3.8+
from pyspark.sql import SparkSession, functions, types, Row
import re
import math


line_re = re.compile(r"^(\S+) - - \[\S+ [+-]\d+\] \"[A-Z]+ \S+ HTTP/\d\.\d\" \d+ (\d+)$")


def line_to_row(line):
    """
    Take a logfile line and return a Row object with hostname and bytes transferred.
    Return None if regex doesn't match.
    """
    m = line_re.match(line)
    if m:
        # TODO
        host = m.group(1)
        byte = m.group(2)
        return Row(hosts=host, bytes=byte)
    else:
        return None


def not_none(row):
    """
    Is this None? Hint: .filter() with it.
    """
    return row is not None


def create_row_rdd(in_directory):
    log_lines = spark.sparkContext.textFile(in_directory)

    # TODO: return an RDD of Row() objects
    Row = log_lines.map(line_to_row).filter(not_none)
    return Row


def main(in_directory):
    logs = spark.createDataFrame(create_row_rdd(in_directory))

    # TODO: calculate r.
    clean_logs = logs.groupBy('hosts').agg({'hosts':'count', 'bytes':'sum'})

    clean_logs = clean_logs.withColumnRenamed('count(hosts)', 'xi')
    clean_logs = clean_logs.withColumn('xi2', clean_logs['xi']**2)

    clean_logs = clean_logs.withColumnRenamed('sum(bytes)', 'yi')
    clean_logs = clean_logs.withColumn('yi2', clean_logs['yi']**2)

    clean_logs = clean_logs.withColumn('xiyi', clean_logs['xi']*clean_logs['yi'])

    total = clean_logs.groupBy().sum().first()
    count = clean_logs.count()
    #total.show()
    #print(total)

    r = (count*total[4] - total[0]*total[1]) / (math.sqrt(count*total[2]-total[0]**2) * math.sqrt(count*total[3]- total[1]**2))
    # TODO: it isn't zero.

    print(f"r = {r}\nr^2 = {r*r}")
    # Built-in function should get the same results.
    #print(totals.corr('count', 'bytes'))


if __name__=='__main__':
    in_directory = sys.argv[1]
    spark = SparkSession.builder.appName('correlate logs').getOrCreate()
    assert spark.version >= '3.2' # make sure we have Spark 3.2+
    spark.sparkContext.setLogLevel('WARN')

    main(in_directory)
