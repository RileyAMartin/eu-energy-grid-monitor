FROM python:3.10-slim
WORKDIR /app
COPY ./eugrid_monitor_core ./eugrid_monitor_core
COPY ./process_app/requirements.txt ./requirements.txt
RUN pip install -r requirements.txt
COPY ./process_app ./process_app
ENV PYTHONPATH /app
CMD ["python", "-m", "process_app.app.main"]