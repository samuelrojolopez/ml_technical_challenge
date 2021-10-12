# coding=utf-8
"""
Challenge High Level Architecture

This script generates the HLA diagram of the solution presented for this
challenge, the script requires the library diagrams to work.

"""

from diagrams import Diagram, Cluster
from diagrams.aws.storage import S3
from diagrams.aws.ml import Sagemaker
from diagrams.aws.analytics import Glue, Redshift
from diagrams.aws.compute import ElasticBeanstalk, Lambda

with Diagram("ML Challenge HLA",
             filename="ml_challenge_hla", show=True):
    raw_data = S3("Raw Data")

    glue_job = Glue("Feature Engineering")
    features_warehouse = Redshift("Creadit Features")
    parquet_features = S3("Credit Features compressed")

    training_job = Sagemaker("Training Job")
    trained_model = S3("Trained Model")

    lambda_feature_retriever = Lambda("Features API")
    sagemaker_endpoint = Sagemaker("Model Endpoint")

    lambda_prediction = Lambda("Predictions API")

    # Nodes Connections
    raw_data >> glue_job >> features_warehouse
    glue_job >> parquet_features >> training_job >> sagemaker_endpoint
    training_job >> trained_model
    features_warehouse >> lambda_feature_retriever >> lambda_prediction
    sagemaker_endpoint >> lambda_prediction

