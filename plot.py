import seaborn as sns
import pandas as pd
import matplotlib.pyplot as plt

avg = pd.read_csv('resource_sum_usage_day/part-00000-352d110a-bf32-4141-8d4f-d10c73858de0-c000.csv')

ax = sns.barplot(avg["dayOfTrace"],avg["sum(resource_request_cpus)"])

ax.set_xticklabels(ax.get_xticklabels(),rotation = 90)
plt.xlabel('Dia')
plt.ylabel('Total de CPU solicitado')

plt.show()

