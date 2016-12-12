# NLA-project-team-24
This is a repository of Skoltech students for their course project for Numerical Linear Algebra course

Proposal (only view): https://docs.google.com/document/d/1cM5BoUBs_xBR2X4KjBJkmiGbCyX4i2PYpDHEkeyBBas/edit?usp=sharing

Main article: https://cs.uwaterloo.ca/~ashraf/theses/XiangMMath13.pdf

Spark tutorial knowledge: 
- Spark web UI: https://www.mapr.com/blog/getting-started-spark-web-ui
- Spark programming guide: http://spark.apache.org/docs/latest/programming-guide.html

Compiling project
1. Put your code to src/scala (use appropriate packages)

2. Make sure, that all external library dependencies are included to simple.sbt

3. In the root directory of the project run 

sbt package

It will download all required labraries and compile your code.

4. It target/scala-2.11.7 you will get jar file, which must be used in spark-submit

