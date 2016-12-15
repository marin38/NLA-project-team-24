import os

master = "10.10.10.10"
for i in range(1, 10):
  os.system("spark-submit --class speedTest NLAproject-assembly-1.0.jar {0} {1}".format(i, master))
