FROM apache/airflow:1.10.12

USER root

ENV KUBE_VERSION="v1.17.0"
ENV HELM_VERSION="v3.0.3"

# Mostly copied from https://github.com/dtzar/helm-kubectl/blob/master/Dockerfile
RUN curl -q https://storage.googleapis.com/kubernetes-release/release/${KUBE_VERSION}/bin/linux/amd64/kubectl -o /usr/local/bin/kubectl \
    && chmod +x /usr/local/bin/kubectl \
    && curl -q https://get.helm.sh/helm-${HELM_VERSION}-linux-amd64.tar.gz -o - | tar -xzO linux-amd64/helm > /usr/local/bin/helm \
    && chmod +x /usr/local/bin/helm

# install jq (for docker image build script)
RUN apt-get update \
  && apt-get install --assume-yes --no-install-recommends \
    jq \
    git \
  && rm -rf /var/lib/apt/lists/*

RUN mkdir -p /usr/local/gcloud \
    && curl -q https://dl.google.com/dl/cloudsdk/release/google-cloud-sdk.tar.gz -o /tmp/google-cloud-sdk.tar.gz \
    && tar -C /usr/local/gcloud -xvf /tmp/google-cloud-sdk.tar.gz \
    && rm /tmp/google-cloud-sdk.tar.gz \
    && /usr/local/gcloud/google-cloud-sdk/install.sh

USER airflow

ENV PATH /usr/local/gcloud/google-cloud-sdk/bin:$PATH

ENV PATH /usr/local/airflow/.local/bin:$PATH

COPY --chown=airflow:airflow requirements.txt ./
RUN pip install --user -r requirements.txt

ARG install_dev="n"
COPY --chown=airflow:airflow requirements.dev.txt ./
RUN if [ "${install_dev}" = "y" ]; then pip install --user -r requirements.dev.txt; fi

COPY --chown=airflow:airflow dags ./dags

ENV DOCKER_SCRIPTS_DIR=/usr/local/airflow/docker
COPY --chown=airflow:airflow docker "${DOCKER_SCRIPTS_DIR}"

ENV HELM_CHARTS_DIR=/usr/local/airflow/helm
COPY --chown=airflow:airflow helm ${HELM_CHARTS_DIR}
RUN cd ${HELM_CHARTS_DIR}/sciencebeam \
  && helm repo add stable https://kubernetes-charts.storage.googleapis.com \
  && helm dep update

COPY --chown=airflow:airflow sciencebeam_airflow ./sciencebeam_airflow
COPY --chown=airflow:airflow setup.py ./
RUN pip install -e . --user --no-dependencies
