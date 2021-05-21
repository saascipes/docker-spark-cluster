import os, sys
import getopt
import traceback
from exceptions import Exception
from pyspark import SparkContext
import re
import csv
from enum import Enum


class WordCountFields(Enum):
    WORD = 0
    COUNT = 1


output_fields_word_count = [
    WordCountFields.WORD,
    WordCountFields.COUNT]


def map_raw_data(line):
    """
    Map raw data
    """
    line = ''.join([x for x in line if (ord(x) < 123 and ord(x) > 96) or (ord(x) < 91 and ord(x) > 64) or ord(x) == 9 or ord(x) == 32])
    line = re.sub(u'\ufffd', '', line).lower()

    rec = ' '.join(line.split()).split(' ')

    return rec


def save_rdd_to_file(rdd, out_file_path, fields, delim=',', append=False):
    fp = os.path.dirname(os.path.realpath(out_file_path))
    if not os.path.exists(fp):
        os.makedirs(fp)

    print('Saving ', out_file_path, ' to ', fp)
    with open(out_file_path, 'a' if append else 'w') as out_file:
        csv.register_dialect('custom', delimiter=delim, skipinitialspace=True, quoting=csv.QUOTE_ALL, escapechar='', quotechar='\"')
        writer = csv.writer(out_file, dialect='custom')

        if not append:
            row_out = []
            for fld in fields:
                print '*'*8, fld
                row_out.append(str(fld).split('.')[1])
            writer.writerow(row_out)

        for row in rdd.collect():
            row_out = []
            for fld in fields:
                row_out.append(row[fld.value])
            writer.writerow(row_out)


def run_word_count(input_path, output_path):
    with SparkContext(appName="WordCountSpark") as sc:
        rdd = sc.textFile(input_path) \
            .flatMap(lambda x: map_raw_data(x)) \
            .map(lambda x: (x, 1)) \
            .filter(lambda x: x != '') \
            .reduceByKey(lambda x, y: x + y)
        save_rdd_to_file(rdd, output_path, output_fields_word_count)
        # for x in rdd.collect():
        #     print x


def main(argv):
    input_path = ''
    output_path = ''

    try:
        opts, args = getopt.getopt(argv, 'hi:o:', '')
        for opt, arg in opts:
            if opt == '-h':
                print 'word-count-spark.py -i <input_path> -o <output_path>'
            elif opt == '-i':
                input_path = arg
            elif opt == '-o':
                output_path = arg

        run_word_count(input_path, output_path)
    except getopt.GetoptError:
        print 'opt error\n'
        print 'word-count-spark.py -i <input_path> -o <output_path>'
        sys.exit(2)
    except Exception:
        traceback.print_exc(file=sys.stdout)
    sys.exit(0)


if __name__ == "__main__":
    main(sys.argv[1:])
