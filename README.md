# Hadoop-E
Hadoop-E is an implementation of the PageRanking algorithm based on Hadoop that works better than Hadopi (you know, the french law)

The goal of this project is to understand how pageranking works with Hadoop Apache solution.

# How to run
First, make sure that you can build the .jar file with the following commands  :
> cd Hadoop-E  
> mvn clean install

then, you just have to use the following command :  
> $HADOOP_ROOT/hadoop jar target/hadoope-1.0-SNAPSHOT.jar main.java.io.github.cr3ahal0.hadoope.HadoopE 

Input files must be (for the moment) in a folder named "files/in" located at the place where you run the program  
Output files are in the folder "files". "ranking" folder (within "files") contains calculation iterations whereas "result" folder contains final results.  

# About
This implementation is based on several articles :
* http://hadoop.apache.org/docs/current/hadoop-mapreduce-client/hadoop-mapreduce-client-core/MapReduceTutorial.html#Example:_WordCount_v2.0
* https://fr.wikipedia.org/wiki/PageRank
* http://blog.xebia.com/wiki-pagerank-with-hadoop/
