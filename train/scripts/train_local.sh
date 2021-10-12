#!/bin/bash

docker build -t ml-challenge-train ../docker
docker run --rm -it -v ../../data/processed_data:/opt/ml/input/data/train -v ../../output/:/opt/ml/model ml-challenge-train
