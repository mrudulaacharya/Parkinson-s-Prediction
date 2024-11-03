import pandas as pd

# Load the dataset
file_path = '/home/risa/Downloads/merged__m_output.csv'
data = pd.read_csv(file_path)

# Display the first few rows 
data.head()

# Analyze the distribution of participants across age groups 
age_group_counts = data['Age_Group'].value_counts().sort_index()

# Display the distribution to understand which groups are biased
age_group_counts

age_bins = [0, 50, 60, 70, 80, 100]
age_labels = ['Under 50', '50-59', '60-69', '70-79', '80 and above']
data['Age_Group'] = pd.cut(data['ENROLL_AGE'], bins=age_bins, labels=age_labels, right=False)

age_group_counts = data['Age_Group'].value_counts().sort_index()
target_size = age_group_counts['70-79']  # Using 70-79 group size as reference for balance

under_sampled_60_69 = data[data['Age_Group'] == '60-69'].sample(n=target_size, random_state=1)

current_count_80_above = age_group_counts['80 and above']
samples_needed = target_size - current_count_80_above
over_sampled_80_above = data[data['Age_Group'] == '80 and above'].sample(n=samples_needed, replace=True, random_state=1)

balanced_data = pd.concat([
    data[data['Age_Group'] != '60-69'],   # Keep other age groups as is
    under_sampled_60_69,                  # Under-sampled 60-69 group
    over_sampled_80_above                 # Over-sampled "80 and above" group
])

balanced_data.to_csv('merged__m_output.csv', index=False)
print("Balanced file saved as 'balanced_data.csv'")