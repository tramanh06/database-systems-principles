__author__ = 'TramAnh'

import matplotlib.pyplot as plt

run_time = [347, 203, 119]
database_size = [1, 0.5, 0.25]

fig = plt.figure()
ax = fig.add_subplot(111)
ax.set_title('Graph of Query Time vs Database size (Query 9)')
ax.set_xlabel('Fraction of Database size')
ax.set_ylabel('Query Time (ms)')

for x, y in zip(database_size, run_time):
    ax.annotate('(%s, %s)' %(x, y), xy = (x+0.02,y))

ax.plot(database_size, run_time, 'ro', database_size, run_time, 'k')        # Plot the point and line
ax.set_xlim(0, 1.2)                 # Increase limit for x axis so can accomodate annotation
ax.set_xticks([0.25, 0.5, 1])       # Set x axis ticks

plt.show()