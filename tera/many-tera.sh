#!/bin/bash


for workers in 1 2 4 8 16 32; do
  ./t2-tera.sh $workers 1
  ./t2-tera.sh $workers 1
#  ./spark-tera.sh $workers 1
#  ./spark-tera.sh $workers 1
done

