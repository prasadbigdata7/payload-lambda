Download Spark:
==============
$ cd ~
$ wget https://d3kbcqa49mib13.cloudfront.net/spark-2.1.1-bin-hadoop2.7.tgz
$ sudo tar -xvf spark-2.1.1-bin-hadoop2.7.tgz
$ cd spark-2.1.1-bin-hadoop2.7
$ bin/spark-shell


create SoftLink (run spark commands from anypath)
===============
$ cd ~
$ ln -s spark-2.1.1-bin-hadoop2.7 spark
$ sudo vi .profile
  export SPARK_HOME = ~/spark
  export PATH=$PATH:$SPARK_HOME/bin

$ .~/.profile


Run JAR file:
=============
$ spark-submit --class Payload_Batch payload-lambda.jar local[2] payload_examples.json


#############################################################################


