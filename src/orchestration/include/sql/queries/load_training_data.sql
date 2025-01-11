SELECT
    {{ feature_columns | join(', ') }}
FROM feature_store.streaming_features
