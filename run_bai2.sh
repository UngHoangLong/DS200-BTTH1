#!/bin/bash
# Compile and run Bai2.java
export HADOOP_HOME=/usr/local/Cellar/hadoop/3.4.3/libexec
export CLASSPATH=$(find $HADOOP_HOME/share/hadoop -name "*.jar" | tr '\n' ':')

javac -cp ".:$CLASSPATH" -d build Bai2.java
rm -rf output_bai2/
java -cp "build:$CLASSPATH" Bai2 data/ratings_1.txt data/ratings_2.txt data/movies.txt output_bai2/
