import seaborn as sns
import pandas as pd
import matplotlib.pyplot as plt

avg = pd.read_csv('jobs_type_frequency_hour/part-00000-77f2cca7-e671-46bb-876a-581850046392-c000.csv')

avg = avg.pivot(
    index=["hourOfTrace"],
    columns="type",
    values="count"
)

totals = list(avg.sum(axis=1))

print(avg)
print()

for idx, total in enumerate(totals):
    print(idx,total)
    avg.iloc[idx] = avg.loc[idx]/total

ax = avg.plot(kind='bar', stacked=True)

ax.set_xticklabels(ax.get_xticklabels(),rotation = 90)
plt.xlabel('Hora')
plt.ylabel('Contagem de Jobs')

plt.show()