import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt

# Load your dataset
df = pd.read_csv('/Users/shalakapadalkar/Desktop/MLOps/Data_Merge/data_modified.csv')

# Step 1: Convert any date-like columns to datetime, or exclude if they are unnecessary
# If there are specific date columns, convert them:
#df['BIRTHDT'] = pd.to_datetime(df['BIRTHDT'], errors='coerce')  # Replace 'date_column' with the actual column name

# Step 2: Drop non-numeric columns for correlation calculation
numeric_df = df.select_dtypes(include=['number'])  # Select only numeric columns

# Step 3: Calculate the correlation matrix on the numeric columns
correlation_matrix = numeric_df.corr()

# Step 4: Display the correlation matrix as a heatmap
plt.figure(figsize=(12, 8))
sns.heatmap(correlation_matrix, annot=False, fmt=".2f", cmap="coolwarm", vmin=-1, vmax=1)
plt.title("Correlation Matrix of Numeric Features")
plt.show()