# ML Challenge

## Description

The code contained in this repository aims to show an MLOPs PoC, where the 
strategy follows and architecture of recurring Feature Processing of a raw
csv file, which loads its outputs into a distribuited storage (such as S3 or
HDFS) in order to be available in a compressed format that can keep its schema
in its metadata. 

The outputs of the Feature Engineering process are also aimed to be deposited
into a Warehouse (such as Hive or Redshift) in order to be available for 
different consumers using SQL Queries of the data they need.

For the consumption of the files in a compressed format (sucha as parquet), 
a training image will consume them using a channel connected into the 
distribuited storage, or using channels for Sagemaker pointing to S3 buckets.

The trained model will be deployed as an enpoint in a running container 
locally, but due the unknown demand of this endpoint a managed Sagemaker 
endpoit will be considered to deliver the predictions of this trained model.

Lastly, in order to get the features and predictions an API that receives an
id will query the latest features corresponding to that id and will send this
features in a single request into the deployed endpoint of the trained model,
receiving the response of the model endpoint and delivering the predicted
outcome.


## High Level Architecture

![ML Challenge HLA Diagram](https://github.com/samuelrojolopez/ml_technical_challenge/blob/main/configuration/diagrams/ml_challenge_hla.png?raw=true)


### Requirements and Installation

#### Docker Compose Cluster

The repository contains 4 shell scripts that will set up a docker-compose 
cluster in order to let a Spark development for the Feature Engineering 
local development. The functions of each shell can be described next:

* start.sh: Docker build of all the containers and set them up and ready to use.
* stop.sh: Stops all the containers without delete any of them
* restart.sh:Stops all the containers and runs them all again
* reset.sh: Deletes all the containers in the machine for a clean start

#### Jupyter Notebook

In order to run locally the Notebooks contained in this repository, you must
use at least Python 3.7.* or higher and use the requirements added to run the
code contained in the notebooks, and the diagrams as code scripts contained 
here.

`pip install -r requirements.txt`

