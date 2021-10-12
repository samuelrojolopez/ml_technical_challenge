# coding=utf-8
"""
ML Challenge High Level Architecture

This scripts generates a Diagram from a diagram as code in order to generate
the High level architechture diagram using python and the diagrams library.

"""

from diagrams import Diagram, Cluster
from diagrams.aws.storage import S3
from diagrams.aws.ml import Sagemaker
from diagrams.aws.analytics import Glue, Redshift
from diagrams.aws.compute import Lambda

with Diagram("ML Challenge High Level Architecture",
             filename="ml_challenge_hla", show=True):

    raw_data = S3("Raw Credit Dataset")
    fe_glue_job = Glue("Feature Engineering")
    fe_warehouse = Redshift("Features Table")
    fe_files = S3("Parquet Features")
    training_job = Sagemaker("Training Job")
    model_artifact = S3("Fraud Model")
    model_endpoint = Sagemaker("Predict Endpoint")
    query_features = Lambda("ID Features")
    is_fraud_api = Lambda("Fraud Predictor")

    # Nodes Connections
    raw_data >> fe_glue_job >> fe_warehouse
    fe_glue_job >> fe_files >> training_job >> model_artifact
    training_job >> model_endpoint
    model_endpoint >> is_fraud_api
    fe_warehouse >> query_features >> is_fraud_api
