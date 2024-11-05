import pandas as pd
import numpy as np
from sklearn.decomposition import PCA
from sklearn.preprocessing import StandardScaler
from sklearn.preprocessing import LabelEncoder

# Load your dataset
data = pd.read_csv('/Users/shalakapadalkar/Desktop/MLOps/Data_Merge/data_modified.csv')

# Label Encoding for `SAAMethod`
label_encoder = LabelEncoder()
data['SAAMethod'] = label_encoder.fit_transform(data['SAAMethod'])

# One-Hot Encoding for `SAA_Status` and `SAA_Type`
data_encoded = pd.get_dummies(data, columns=['SAA_Status', 'SAA_Type'])

# View the encoded DataFrame to confirm
print(data_encoded.head())

# Separate features and target variable
X = data_encoded.drop(columns=['COHORT', 'PDSTATE'])  # replace 'target_column' with your target column name

# Standardize features
scaler = StandardScaler()
X_scaled = scaler.fit_transform(X)

# Initialize PCA with the desired number of components
pca = PCA(n_components=70)
X_pca = pca.fit_transform(X_scaled)

# Check the explained variance ratio
print("PCA Result:\n", X_pca)
print("Explained Variance Ratio:", pca.explained_variance_ratio_)
print("Cumulative Explained Variance:", np.cumsum(pca.explained_variance_ratio_))

# Generate column names dynamically for 70 components
column_names = [f'PC{i+1}' for i in range(70)]

# Create the DataFrame with dynamically generated column names
pca_df = pd.DataFrame(X_pca, columns=column_names)

# View the DataFrame to confirm
print(pca_df.head())

# Save to CSV
pca_df.to_csv('pca_components.csv', index=False)

print("Principal components saved to 'pca_components.csv'")

# Convert to DataFrame with component labels
explained_variance_df = pd.DataFrame({
    'Principal Component': [f'PC{i+1}' for i in range(len(pca.explained_variance_ratio_))],
    'Explained Variance Ratio': pca.explained_variance_ratio_
})

# Sort by explained variance ratio in ascending order
explained_variance_df_sorted = explained_variance_df.sort_values(by='Explained Variance Ratio', ascending=True).reset_index(drop=True)

# Print the sorted DataFrame
print('Sorted explained variance:',explained_variance_df_sorted)