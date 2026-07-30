FROM spark:4.1.2-python3

USER root

COPY spark_jobs/requirements.txt /tmp/coinwhale-spark-requirements.txt
RUN python3 -m pip install --no-cache-dir \
    -r /tmp/coinwhale-spark-requirements.txt \
    && rm /tmp/coinwhale-spark-requirements.txt

COPY --chown=185:185 spark_jobs /opt/spark/work-dir
RUN mkdir -p /opt/spark/work-dir/checkpoints /opt/spark/work-dir/dlq \
    && chown -R 185:185 /opt/spark/work-dir

USER 185
