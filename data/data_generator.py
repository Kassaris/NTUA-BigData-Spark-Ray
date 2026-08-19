import pandas as pd
import numpy as np
from sklearn.datasets import make_classification
from faker import Faker
import sys
import os


faker=Faker()

def generate_mixed_data_chunk(chunk_size):
    # Generate synthetic data with numeric features
    numeric_features, labels = make_classification(
        n_samples=chunk_size,
        n_features=4,
        n_informative=4,
        n_redundant=0,
        random_state=123
    )

    numeric_df = pd.DataFrame(data=numeric_features, columns=[f'feature_{i+1}' for i in range(4)])

    categorical_df = pd.DataFrame({
        'categorical_feature_1': np.random.randint(1, chunk_size, size=chunk_size),
        'categorical_feature_2': np.random.randint(1, chunk_size//2, size=chunk_size),
        'word': [faker.word() for _ in range(chunk_size)]
    })

    mixed_df = pd.concat([numeric_df, categorical_df], axis=1)

    df_chunk = pd.concat([mixed_df, pd.DataFrame({'label': labels})], axis=1)

    return df_chunk

def create_mixed_large_csv(num_samples, chunk_size=1000):
        header = [f'feature_{i+1}' for i in range(4)] + ['categorical_feature_1', 'categorical_feature_2','word', 'label']

        print(','.join(header))
        for _ in range(0, num_samples, chunk_size):
            data_chunk = generate_mixed_data_chunk(chunk_size)
            data_chunk.to_csv(sys.stdout, header=False, index=False, mode='a')
            sys.stdout.flush()
          
num_samples = int(sys.argv[1]) 
create_mixed_large_csv(num_samples=num_samples)
