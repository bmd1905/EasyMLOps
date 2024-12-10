def remove_nulls(dataset):
    """
    Remove nulls from the dataset.
    """
    return dataset.filter(lambda x: x is not None)


def split_data(dataset):
    """
    Split the dataset into train and validation datasets.
    """
    return dataset.train_test_split(test_size=0.3)
