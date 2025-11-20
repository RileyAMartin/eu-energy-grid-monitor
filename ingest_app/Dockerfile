FROM python:3.10-slim
WORKDIR /app
COPY ./eugrid_monitor_core ./eugrid_monitor_core
COPY ./ingest_app/requirements.txt ./requirements.txt
RUN pip install -r requirements.txt
COPY ./ingest_app ./ingest_app
ENV PYTHONPATH /app
CMD ["python", "-m", "ingest_app.app.main"]