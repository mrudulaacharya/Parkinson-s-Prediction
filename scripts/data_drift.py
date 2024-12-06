# -*- coding: utf-8 -*-
"""Untitled5.ipynb

Automatically generated by Colab.

Original file is located at
    https://colab.research.google.com/drive/1Q3WHNg60QVzl4p1B6pnat5G3bI-C8xT_
"""

import pandas as pd
from google.colab import files

import pandas as pd

# Load the uploaded dataset
file_path = 'data.csv'
data = pd.read_csv(file_path)

# Calculate basic statistics: mean, median, and mode
statistics = data.describe().transpose()
statistics['median'] = data.median()
statistics['mode'] = data.mode().iloc[0]

# Display the calculated statistics
print("Feature Statistics Overview:")
print(statistics)

import matplotlib.pyplot as plt
import seaborn as sns

# Box Plot for all numerical columns
plt.figure(figsize=(10, 6))
sns.boxplot(data=data)
plt.title('Box Plot of Features')
plt.xticks(rotation=45)
plt.show()

# Histogram for each column
data.hist(bins=20, figsize=(14, 10), grid=False)
plt.suptitle('Histograms of Features')
plt.show()

# Bar Plot for Mean, Median, Mode
summary_stats = statistics[['mean', 'median', 'mode']]
summary_stats.plot(kind='bar', figsize=(12, 6))
plt.title('Summary Statistics (Mean, Median, Mode)')
plt.ylabel('Value')
plt.xlabel('Features')
plt.xticks(rotation=45)
plt.legend(title="Statistics")
plt.show()