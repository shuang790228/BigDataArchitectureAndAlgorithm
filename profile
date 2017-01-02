export JAVA_HOME=/Library/Java/JavaVirtualMachines/jdk1.8.0_112.jdk/Contents/Home

export PATH=/usr/bin:/bin:/sbin:/usr/sbin:/usr/local/bin

export TOMCAT_HOME=/Users/huangsean/Coding/apache-tomcat-8.5.3
export PATH=$PATH:$TOMCAT_HOME/bin

export NGINX_HOME=/usr/local/nginx
export PATH=$PATH:$NGINX_HOME/sbin

export HADOOP_HOME=/Users/huangsean/Coding/hadoop-2.7.3
export PATH=$PATH:$HADOOP_HOME/bin
export PATH=$PATH:$HADOOP_HOME/sbin
export HADOOP_MAPRED_HOME=$HADOOP_HOME
export HADOOP_COMMON_HOME=$HADOOP_HOME
export HADOOP_HDFS_HOME=$HADOOP_HOME
export YARN_HOME=$HADOOP_HOME
export HADOOP_COMMON_LIB_NATIVE_DIR=$HADOOP_HOME/lib/native
export HADOOP_OPTS="-Djava.library.path=$HADOOP_HOME/lib"

export FLUME_HOME=/Users/huangsean/Coding/apache-flume-1.7.0-bin
export PATH=$FLUME_HOME/bin:$PATH

export HIVE_HOME=/Users/huangsean/Coding/apache-hive-2.1.0-bin
export PATH=$HIVE_HOME/bin:$PATH

export ZOOKEEPER_HOME=/Users/huangsean/Coding/zookeeper-3.4.9
export PATH=$ZOOKEEPER_HOME/bin:$PATH

export KAFKA_HOME=/Users/huangsean/Coding/kafka_2.11-0.10.1.0
export PATH=$KAFKA_HOME/bin:$PATH

export STORM_HOME=/Users/huangsean/Coding/apache-storm-1.0.2
export PATH=$STORM_HOME/bin:$PATH
