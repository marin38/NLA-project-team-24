import numpy as np

n_range = range(10, 100, 10)

for n in n_range:
  print(n)
  matrix = np.random.rand(n * 1000, n * 1)
  np.savetxt("data/matrix_m_{0}".format(1000 * n), matrix)
  
