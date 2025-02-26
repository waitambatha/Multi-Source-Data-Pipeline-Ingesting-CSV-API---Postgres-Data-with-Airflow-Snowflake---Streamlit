import pandas as pd
import numpy as np
import os

# Ensure the data directory exists
os.makedirs('data', exist_ok=True)

columns = ['school_id', 'name', 'address', 'city', 'state', 'zip', 'phone', 'type', 'num_students', 'rating']
data = []

for i in range(500):
    data.append({
        'school_id': i + 1,
        'name': f'School {i + 1}',
        'address': f'{i + 1} Main St',
        'city': 'CityName',
        'state': 'StateName',
        'zip': f'{10000 + i}',
        'phone': f'555-010{i % 10}',
        'type': 'Public' if i % 2 == 0 else 'Private',
        'num_students': np.random.randint(100, 1000),
        'rating': np.random.randint(1, 5)
    })

df = pd.DataFrame(data)
df.to_csv('data/schools.csv', index=False)
print("CSV file generated at data/schools.csv")
