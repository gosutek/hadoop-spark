#!/bin/bash

scriptname=$0

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
      echo " 1 - Prints rows and datatype of Crime Data DataFrame. Output \
        in output-1.txt"
      echo " 2 - Run query 1. Output in output-2.txt"
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
    /home/master/opt/spark/bin/spark-submit \
      row-dtype.py > output-1.txt
    ;;
  2)
    echo "Running script 2"
    ;;
esac

