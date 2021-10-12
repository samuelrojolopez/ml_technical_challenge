#!/bin/bash

docker build -t ml-challenge-train train/docker
docker run --rm -it -v $(pwd)/data/processed_data:/opt/ml/input/data/train -v $(pwd)/output:/opt/ml/model ml-challenge-train
