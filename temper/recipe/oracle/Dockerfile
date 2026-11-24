# The workload and oracle for AutoMQ's scenarios: confluent-kafka producers
# write a ledger of every send's outcome, and a consumer reads every
# partition back.
FROM python:3.12-slim
RUN pip install --no-cache-dir confluent-kafka==2.12.2
COPY oracle /oracle/oracle
WORKDIR /oracle
ENTRYPOINT []
