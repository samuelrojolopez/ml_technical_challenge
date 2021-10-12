FROM continuumio/miniconda3 AS build

COPY environment.yml .
RUN conda env create -f environment.yml

RUN conda config --set always_yes yes --set changeps1 no && \
  conda install -c conda-forge conda-pack

RUN conda-pack -n credit-risk -o /tmp/env.tar && \
  mkdir /venv && cd /venv && tar xf /tmp/env.tar && \
  rm /tmp/env.tar

RUN /venv/bin/conda-unpack

FROM ubuntu:20.04 as runtime

COPY --from=build /venv /venv

ENV PYTHONUNBUFFERED=TRUE
ENV PYTHONDONTWRITEBYTECODE=TRUE

COPY src /opt/ml/code
RUN chmod +x /opt/ml/code/run.sh

SHELL ["/bin/bash", "-c"]
ENTRYPOINT source /venv/bin/activate && \
  /opt/ml/code/run.sh
