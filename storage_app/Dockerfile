FROM python:3.10-slim
WORKDIR /app
COPY ./eugrid_monitor_core ./eugrid_monitor_core
COPY ./storage_app/requirements.txt ./requirements.txt
RUN pip install -r requirements.txt
COPY ./storage_app ./storage_app
ENV PYTHONPATH /app
CMD ["python", "-m", "storage_app.app.main"]