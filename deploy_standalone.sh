cluster_hostname=team24@nla24project1-ssh.azurehdinsight.net

if [ ! $# == 1 ]
then
  echo "Please specify class name"
  exit
fi

sbt package && /home/anton/tools/spark-2.0.2-bin-hadoop2.7/bin/spark-submit --class "$1" --master local[4] target/scala-2.11.7/nla-project_2.11.7-1.0.jar > output.txt && cat output.txt
