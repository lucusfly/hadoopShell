#!/bin/bash

. common.sh

HADOOPJAR="$HADOOP_HOME/share/hadoop/tools/lib/hadoop-streaming-2.3.0-cdh5.0.2.jar"
#HADOOPJAR="$HADOOP_HOME/share/hadoop/tools/lib/hadoop-streaming-2.5.0-cdh5.2.0.jar"
ExistHD() {
    # 0 exists. 1 not exists.
    if [ $1 ];then
        hadoop fs -test -e $1
        if [ $? -eq 0 ];then
            return 0
        fi
    fi
    return 1
}

GetMergeHD() {
    #merge and delete crc file
    if [ $1 ] && [ $2 ];then
        hdfs_new_data=$1
        local_data=$2
        Remove $local_data
        ExistHD $hdfs_new_data
        if [ $? -eq 0 ];then
            echo "getmerge $hdfs_new_data to $local_data"
            hadoop fs -getmerge $hdfs_new_data $local_data

            dotFile="$(dirname $local_data)/.$(basename $local_data).crc"
            Remove $dotFile
        fi
    fi
}

RemoveHD() {
    #remove if exist
    if [ $1 ];then
        ExistHD $1
        if [ $? -eq 0 ];then
            echo "remove $1 from hadoop"
            hadoop fs -rm -r $1
        fi
    fi
}

MkdirHD() {
    #mkdir if not exist
    if [ $1 ];then
        ExistHD $1
        if [ $?  -ne 0 ];then
            echo "mkdir $1 from hadoop"
            hadoop fs -mkdir $1
        fi
    fi
}

#mapreduce without reduce
MapHD() {
    #1 input path, 2 out_path, 3 mapFile, 4 map reduce name
    hadoop jar $HADOOPJAR \
    -D mapreduce.job.name="$4" \
    -D mapred.skip.mode.enabled=true \
    -D mapreduce.map.skip.maxrecords=1 \
    -D mapreduce.task.skip.start.attempts=2 \
    -D mapreduce.job.reduces=0 \
    -input $1 \
    -output $2 \
    -file  $3 \
    -mapper "python $3"
}

#map reduce
MapReduceHD() {
     #1 input path, 2 out_path, 3 mapFile, 4 reduceFile, 5 map reduce name
     hadoop jar $HADOOPJAR \
     -D mapreduce.job.name="$5" \
     -D mapreduce.map.skip.maxrecords=1 \
     -D mapreduce.task.skip.start.attempts=2 \
     -D mapred.skip.mode.enabled=true \
     -files $3,$4 \
     -input $1 \
     -output $2 \
     -mapper "python $3" \
     -reducer "python $4"
}

ErrorExitHD() {
    #param 1 success dir
    if [ $1 ];then
        ExistHD "$1/_SUCCESS"
        if [ $? -ne 0 ];then
            echo "job failed"
            exit 1
        fi
    fi
}
