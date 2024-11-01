import pandas as pd

# Load the CSV file
df = pd.read_csv('merged__m_output.csv')

# List of columns to drop
columns_to_drop = ['ENROLL_DATE', 'EVENT_ID_x', 'PAG_NAME_x', 'INFODT_x', 'PATNO', 'RUNDATE', 'PROJECTID', 'EVENT_ID_y', 'INFODT_y', 'ORIG_ENTRY', 'LAST_UPDATE', 'ORIG_ENTRY.1', 'LAST_UPDATE.1', 'ORIG_ENTRY.2', 'LAST_UPDATE.2', 'PDMEDDT', 'PDMEDTM', 'EXAMDT', 'EXAMTM', 'ORIG_ENTRY.3', 'LAST_UPDATE.3', 'REC_ID', 'PAG_NAME_y']

# Drop the specified columns
df = df.drop(columns=columns_to_drop)

# Save the modified DataFrame back to a new CSV file (or overwrite the original)
df.to_csv('data_modified.csv', index=False)

print("Selected columns dropped and saved as 'data_modified.csv'")