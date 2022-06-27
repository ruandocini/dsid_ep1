import seaborn as sns
import pandas as pd
import matplotlib.pyplot as plt

avg = pd.read_csv('jobsFailedPerTier/part-00000-f248992a-c264-4b5d-9efa-531c8f2299f6-c000.csv')

# avg.sort_values(by='timeGapJobTask',ascending=True)

# avg = avg.head(100)

ax = sns.barplot(x=avg["jobTier"],y=avg["count"])
ax.set_xticklabels(ax.get_xticklabels(),rotation=0)

plt.xlabel('Tier do Job')
plt.ylabel('Contagem de falhas')

plt.show()
# avg = avg.pivot(
#     index=["dayOfTrace"],
#     columns="type",
#     values="count"
# )

# totals = list(avg.sum(axis=1))

# print(avg)
# print()

# for idx, total in enumerate(totals):
#     print(idx,total)
#     avg.iloc[idx] = avg.loc[idx]/total

# ax = avg.plot(kind='bar', stacked=True)


# plt.show()