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

if [ $# -eq 0 ]; then
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
		echo " 2 - Query 1 using Dataframe API."
		echo " 3 - Query 1 using SQL API."
		echo " 4 - Query 2 using Dataframe API."
		echo " 5 - Query 2 using RDD API."
		echo " 6 - Query 3 using 2 executors."
		echo " 7 - Query 3 using 3 executors."
		echo " 8 - Query 3 using 4 executors."
		echo " 9 - Query 4."
		echo "Do run.sh [-s [1...9]] to execute."
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
		$SCRIPTS_PATH/preprocessing.py >$OUTPUT_PATH/output-preprocessing.txt
	;;
2)
	$SPARK_HOME/bin/spark-submit \
		--deploy-mode client \
		--master yarn \
		--num-executors 4 \
		$SCRIPTS_PATH/query-1_dataframe.py >$OUTPUT_PATH/output-1_dataframe.txt
	;;
3)
	$SPARK_HOME/bin/spark-submit \
		--deploy-mode client \
		--master yarn \
		--num-executors 4 \
		$SCRIPTS_PATH/query-1_sql.py >$OUTPUT_PATH/output-1_sql.txt
	;;
4)
	$SPARK_HOME/bin/spark-submit \
		--deploy-mode client \
		--master yarn \
		--num-executors 4 \
		$SCRIPTS_PATH/query-2_dataframe.py >$OUTPUT_PATH/output-2_dataframe.txt
	;;
5)
	$SPARK_HOME/bin/spark-submit \
		--deploy-mode client \
		--master yarn \
		--num-executors 4 \
		$SCRIPTS_PATH/query-2_RDD.py >$OUTPUT_PATH/output-2_RDD.txt
	;;
6)
	$SPARK_HOME/bin/spark-submit \
		--deploy-mode client \
		--master yarn \
		--num-executors 2 \
		$SCRIPTS_PATH/query-3.py >$OUTPUT_PATH/output-3.txt
	;;
7)
	$SPARK_HOME/bin/spark-submit \
		--deploy-mode client \
		--master yarn \
		--num-executors 3 \
		$SCRIPTS_PATH/query-3.py >$OUTPUT_PATH/output-3.txt
	;;
8)
	$SPARK_HOME/bin/spark-submit \
		--deploy-mode client \
		--master yarn \
		--num-executors 4 \
		$SCRIPTS_PATH/query-3.py >$OUTPUT_PATH/output-3.txt
	;;
9)
	$SPARK_HOME/bin/spark-submit \
		--deploy-mode client \
		--master yarn \
		--num-executors 4 \
		$SCRIPTS_PATH/query-4.py >$OUTPUT_PATH/output-4.txt
	;;
esac
