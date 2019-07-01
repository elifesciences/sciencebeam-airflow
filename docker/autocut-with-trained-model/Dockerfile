ARG base_image
FROM ${base_image}

COPY model.pkl /opt/model.pkl
ENV AUTOCUT_MODEL_PATH=/opt/model.pkl

CMD ["start-autocut.sh"]
