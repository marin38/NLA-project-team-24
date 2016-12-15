cluster_hostname=team24@nlask24-ssh.azurehdinsight.net

if [ ! $# == 1 ]
then
  echo "Please specify class name"
  exit
fi

sbt package && scp target/scala-2.11.7/nla-project_2.11.7-1.0.jar $cluster_hostname:/home/team24 && ssh $cluster_hostname spark-submit --class "$1" --master yarn nla-project_2.11.7-1.0.jar > output.txt && cat output.txt
