import seaborn as sns
import pandas as pd
import matplotlib.pyplot as plt

avg = pd.read_csv('tasks_day/part-00000-89fcce30-b844-4397-9f5d-40ce21c54df2-c000.csv')

ax = sns.barplot(avg["dayOfTrace"],avg["count"])

ax.set_xticklabels(ax.get_xticklabels(),rotation = 90)
plt.xlabel('Dia')
plt.ylabel('Contagem de Tasks')

plt.show()

