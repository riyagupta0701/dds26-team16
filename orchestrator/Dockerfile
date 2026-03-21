FROM python:3.12-slim
WORKDIR /home/orchestrator
COPY ./requirements.txt .
RUN pip install -r requirements.txt
COPY . .
CMD ["python", "-u", "app.py"]
