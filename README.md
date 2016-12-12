# NLA-project-team-24
This is a repository of Skoltech students for their course project for Numerical Linear Algebra course

Proposal (only view): https://docs.google.com/document/d/1cM5BoUBs_xBR2X4KjBJkmiGbCyX4i2PYpDHEkeyBBas/edit?usp=sharing

Main article: https://cs.uwaterloo.ca/~ashraf/theses/XiangMMath13.pdf

Spark tutorial knowledge: 
- Spark web UI: https://www.mapr.com/blog/getting-started-spark-web-ui
- Spark programming guide: http://spark.apache.org/docs/latest/programming-guide.html

### Compiling and running project

**Important!**
Please, don't write your code on cluster, as it can be suddenly overwritten by somebody else. Moreover, there is some unfixed problem with sbt on cluster. Futhermore, it takes a lot of resources to compile code on cluster. So, **please compile the project locally!**

-----

1. Put your code to src/scala (use appropriate packages)

2. Make sure, that all external library dependencies are included to ***simple.sbt***

3. In the root directory of the project run 
**sbt package**

It will download all required libraries and compile the project.

4. In ***target/scala-2.11.7*** you will get jar file, which you have to copy to the cluster and use in spark-submit

5. You can use deploy.sh to compile and run the project. There is a template of Spark application (it just prints "Hello, world!"), see ***src/scala/template/YorClassName.scala***

Check, that ***deploy.sh*** is executable and just print 

**deploy.sh template.YourClassName**

to compile, copy jar to cluster and run it.

**Note** If you are tired of entering passwords, type the following:
**ssh-keygen**

This will create file "id_rsa.pub" in your ~/.ssh folder. Append it's content to /home/team24/.ssh/authorized_hosts on cluster.

