#!/bin/bash
# Compile and run Bai3.java
export HADOOP_HOME=/usr/local/Cellar/hadoop/3.4.3/libexec
export CLASSPATH=$(find $HADOOP_HOME/share/hadoop -name "*.jar" | tr '\n' ':')

javac -cp ".:$CLASSPATH" -d build Bai3.java
rm -rf output_bai3/
java -cp "build:$CLASSPATH" Bai3 data/ratings_1.txt data/ratings_2.txt data/movies.txt data/users.txt output_bai3/
