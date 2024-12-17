SELECT
    {{ feature_columns | join(', ') }}
FROM dwh.vw_ml_purchase_prediction
