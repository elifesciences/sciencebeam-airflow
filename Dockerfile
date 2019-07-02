FROM puckel/docker-airflow:1.10.2

USER root

ENV KUBE_VERSION="v1.13.4"
ENV HELM_VERSION="v2.14.1"

# Mostly copied from https://github.com/dtzar/helm-kubectl/blob/master/Dockerfile
RUN curl -q https://storage.googleapis.com/kubernetes-release/release/${KUBE_VERSION}/bin/linux/amd64/kubectl -o /usr/local/bin/kubectl \
    && chmod +x /usr/local/bin/kubectl \
    && curl -q https://storage.googleapis.com/kubernetes-helm/helm-${HELM_VERSION}-linux-amd64.tar.gz -o - | tar -xzO linux-amd64/helm > /usr/local/bin/helm \
    && chmod +x /usr/local/bin/helm

RUN mkdir -p /usr/local/gcloud \
    && curl -q https://dl.google.com/dl/cloudsdk/release/google-cloud-sdk.tar.gz -o /tmp/google-cloud-sdk.tar.gz \
    && tar -C /usr/local/gcloud -xvf /tmp/google-cloud-sdk.tar.gz \
    && rm /tmp/google-cloud-sdk.tar.gz \
    && /usr/local/gcloud/google-cloud-sdk/install.sh

# gcloud sdk cli only works with Python 2 (in 2019)
# also install jq (for docker image build script)
RUN apt-get update \
  && apt-get install --assume-yes --no-install-recommends \
    python2.7-minimal libpython2.7-stdlib \
    jq \
  && rm -rf /var/lib/apt/lists/*

USER airflow

ENV PATH /usr/local/gcloud/google-cloud-sdk/bin:$PATH

ENV PATH /usr/local/airflow/.local/bin:$PATH

COPY --chown=airflow:airflow requirements.txt ./
RUN pip install --user -r requirements.txt

ARG install_dev
COPY --chown=airflow:airflow requirements.dev.txt ./
RUN if [ "${install_dev}" = "y" ]; then pip install --user -r requirements.dev.txt; fi

COPY --chown=airflow:airflow dags ./dags

ENV DOCKER_SCRIPTS_DIR=/usr/local/airflow/docker
COPY --chown=airflow:airflow docker "${DOCKER_SCRIPTS_DIR}"

ENV HELM_CHARTS_DIR=/usr/local/airflow/helm
COPY --chown=airflow:airflow helm ${HELM_CHARTS_DIR}
