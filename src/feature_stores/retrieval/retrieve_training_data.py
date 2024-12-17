from datetime import datetime

import constants
import pandas as pd
import pytz
from feast import FeatureStore

# Note: see https://docs.feast.dev/getting-started/concepts/feature-retrieval for
# more details on how to retrieve for all entities in the offline store instead
entity_df = pd.DataFrame.from_dict(
    {
        # entity's join key -> entity values
        "device_id": [0, 1, 2],
    }
)
entity_df["event_timestamp"] = datetime.now(pytz.utc)

# Initialize the feature store
store = FeatureStore(repo_path=constants.REPO_PATH)

# Get training data from feature store
# we only want to get feature_6 for all device_ids
training_df = store.get_historical_features(
    entity_df=entity_df,
    features=[
        "device_stats:feature_0",
    ],
).to_df()

print("----- Feature schema -----\n")
print(training_df.info())

print()
print("----- Example features -----\n")
print(training_df.head())
