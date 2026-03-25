#!/bin/bash
# Compile and run Bai1.java
export HADOOP_HOME=/usr/local/Cellar/hadoop/3.4.3/libexec
export CLASSPATH=$(find $HADOOP_HOME/share/hadoop -name "*.jar" | tr '\n' ':')

# Compile
javac -cp ".:$CLASSPATH" -d build Bai1.java

# Remove old output
rm -rf output_bai1/

# Run
java -cp "build:$CLASSPATH" Bai1 data/ratings_1.txt data/ratings_2.txt data/movies.txt output_bai1/
