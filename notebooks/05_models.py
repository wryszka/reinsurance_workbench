# Databricks notebook source
# MAGIC %md
# MAGIC # 05 · Models + serving — triage/appetite + technical pricing
# MAGIC
# MAGIC Trains two MLflow models off the UC Feature Store (`feature_submission`), registers them to Unity Catalog
# MAGIC with a `champion` alias, and serves each behind a scale-to-zero endpoint. Feature-vector contract (no online
# MAGIC store): the UC-function tools pre-fetch the feature struct and pass it to `ai_query`.
# MAGIC
# MAGIC - `model_triage_classifier` — appetite decision {fast_track, refer, decline} (probabilities).
# MAGIC - `model_loss_ratio` — expected (burning-cost) loss ratio, for technical pricing + rate adequacy.
# MAGIC
# MAGIC The crux (accumulation + capital) is a separate deterministic module (05b) — models do not recompute it.

# COMMAND ----------

dbutils.widgets.text("catalog", "lr_dev_aws_us_catalog")
dbutils.widgets.text("schema", "bricksurance_re")
catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
fqn = f"{catalog}.{schema}"

import mlflow, pandas as pd, numpy as np
from mlflow.models.signature import infer_signature
from databricks.feature_engineering import FeatureEngineeringClient, FeatureLookup
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.serving import ServedEntityInput, EndpointCoreConfigInput
import lightgbm as lgb

mlflow.set_registry_uri("databricks-uc")
fe = FeatureEngineeringClient()
client = mlflow.tracking.MlflowClient()

FEATURES = ["subject_premium_eur", "ceded_share_pct", "rol_pct", "as_if_loss_ratio", "large_loss_count",
            "total_tiv_eur", "data_quality_score", "rate_adequacy", "is_cat_xol", "credit_quality_step",
            "counterparty_pd_pct", "is_peak_zone", "zone_utilisation_pct", "expected_loss_ratio"]
TRIAGE_CLASSES = ["fast_track", "refer", "decline"]

# COMMAND ----------

# MAGIC %md ## Labels (deterministic appetite rules) + training frame

# COMMAND ----------

feat = spark.table(f"{fqn}.feature_submission").toPandas()

def triage_label(r):
    if r["data_quality_score"] < 0.75 or r["rate_adequacy"] < 0.85:
        return "decline"
    if (r["is_peak_zone"] == 1 and r["is_cat_xol"] == 1) or r["credit_quality_step"] >= 4 or r["zone_utilisation_pct"] >= 90:
        return "refer"
    return "fast_track"

feat["triage_label"] = feat.apply(triage_label, axis=1)
feat["triage_y"] = feat["triage_label"].map({c: i for i, c in enumerate(TRIAGE_CLASSES)})
print(feat["triage_label"].value_counts().to_dict())

X = feat[FEATURES]
mlflow.autolog(disable=True)

# COMMAND ----------

# MAGIC %md ## Train + register triage classifier (pyfunc → probability array)

# COMMAND ----------

clf = lgb.LGBMClassifier(n_estimators=200, learning_rate=0.08, num_leaves=16,
                         min_child_samples=3, random_state=42)
clf.fit(X, feat["triage_y"])

class TriageModel(mlflow.pyfunc.PythonModel):
    def __init__(self, model, features, n_classes):
        self.model = model; self.features = features; self.n_classes = n_classes
    def predict(self, context, model_input):
        df = model_input[self.features] if all(f in model_input for f in self.features) else model_input
        proba = self.model.predict_proba(df)
        # ensure full class width even if training saw a subset
        out = np.zeros((proba.shape[0], self.n_classes))
        out[:, self.model.classes_.astype(int)] = proba
        return out.tolist()

sig_t = infer_signature(X.head(3), [[0.7, 0.2, 0.1]] * 3)
with mlflow.start_run(run_name="triage_classifier"):
    mi_t = mlflow.pyfunc.log_model(
        artifact_path="model", python_model=TriageModel(clf, FEATURES, len(TRIAGE_CLASSES)),
        signature=sig_t, input_example=X.head(3),
        pip_requirements=["lightgbm", "scikit-learn", "pandas", "numpy"],
        registered_model_name=f"{fqn}.model_triage_classifier")
tv = mi_t.registered_model_version
client.set_registered_model_alias(f"{fqn}.model_triage_classifier", "champion", tv)
print("triage champion v", tv)

# COMMAND ----------

# MAGIC %md ## Train + register loss-ratio regressor (pyfunc → double)

# COMMAND ----------

reg = lgb.LGBMRegressor(n_estimators=200, learning_rate=0.08, num_leaves=16,
                        min_child_samples=3, random_state=42)
reg.fit(X, feat["expected_loss_ratio"])

class LossRatioModel(mlflow.pyfunc.PythonModel):
    def __init__(self, model, features):
        self.model = model; self.features = features
    def predict(self, context, model_input):
        df = model_input[self.features] if all(f in model_input for f in self.features) else model_input
        return self.model.predict(df).tolist()

sig_r = infer_signature(X.head(3), [0.5, 0.5, 0.5])
with mlflow.start_run(run_name="loss_ratio_regressor"):
    mi_r = mlflow.pyfunc.log_model(
        artifact_path="model", python_model=LossRatioModel(reg, FEATURES),
        signature=sig_r, input_example=X.head(3),
        pip_requirements=["lightgbm", "scikit-learn", "pandas", "numpy"],
        registered_model_name=f"{fqn}.model_loss_ratio")
rv = mi_r.registered_model_version
client.set_registered_model_alias(f"{fqn}.model_loss_ratio", "champion", rv)
print("loss_ratio champion v", rv)

# COMMAND ----------

# MAGIC %md ## Serve both behind scale-to-zero endpoints

# COMMAND ----------

w = WorkspaceClient()

def serve(endpoint, model_fqn, version):
    entity = ServedEntityInput(name="m", entity_name=model_fqn, entity_version=version,
                               workload_size="Small", scale_to_zero_enabled=True)
    existing = [e.name for e in w.serving_endpoints.list()]
    if endpoint in existing:
        w.serving_endpoints.update_config_and_wait(name=endpoint, served_entities=[entity])
    else:
        w.serving_endpoints.create_and_wait(name=endpoint, config=EndpointCoreConfigInput(name=endpoint, served_entities=[entity]))
    print("served", endpoint, "v", version)

serve("reinsurance-triage", f"{fqn}.model_triage_classifier", tv)
serve("reinsurance-pricing", f"{fqn}.model_loss_ratio", rv)
print("serving endpoints ready")
