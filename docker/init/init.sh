#!/bin/sh

set -e

# airflow user id
USER_ID=1000
echo 'changing ownership to $USER_ID, and...'

echo 'copying gcloud credentials...'

cp -r /root/user-config-gcloud/* /root/volume-config-gcloud
chown -R $USER_ID:$USER_ID /root/volume-config-gcloud

echo 'copying kube config...'

cp -r /root/user-config-kube/* /root/volume-config-kube
chown -R $USER_ID:$USER_ID /root/volume-config-kube

echo 'replacing gcloud path in kube config...'

sed -i 's:\S*/gcloud:gcloud:g' /root/volume-config-kube/config

echo 'done'
