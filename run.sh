#!/bin/bash

scriptname=$0
SCRIPTS_PATH="./scripts"
SPARK_HOME="/home/master/opt/spark"
OUTPUT_PATH="./output"

function usage {
  echo "Usage: $scriptname [-s scriptname]"
  echo "    -s [1...5] - specify script to run"
  echo "    -l - list available scripts"
}

if [ $# -eq 0 ];
then
  usage
  exit 1
fi

mkdir -p ./output

while getopts "s:l" option; do
  case ${option} in
    s)
      script=${OPTARG}
      ;;
    l)
      echo "Available scripts:"
      echo " 1 - Preprocessing script."
      echo "Do test.sh [-s [1...5]] to execute"
      ;;
    :)
      usage
      exit 1
      ;;
    ?)
      usage
      exit 1
      ;;
  esac
done

case ${script} in
  1)
    $SPARK_HOME/bin/spark-submit \
	$SCRIPTS_PATH/preprocessing.py > $OUTPUT_PATH/output-preprocessing.txt
    ;;
  2)
    $SPARK_HOME/bin/spark-submit \
	$SCRIPTS_PATH/query-1_dataframe.py > $OUTPUT_PATH/output-1_dataframe.txt
    ;;
  3)
    $SPARK_HOME/bin/spark-submit \
	$SCRIPTS_PATH/query-1_sql.py > $OUTPUT_PATH/output-1_sql.txt
    ;;
esac
