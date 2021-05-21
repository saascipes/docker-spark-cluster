#!/bin/bash

/spark/bin/spark-submit word-count-spark.py -i /opt/spark-data/shakespeare.txt -o results.txt
