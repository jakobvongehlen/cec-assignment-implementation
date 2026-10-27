FROM python:3.12-slim

WORKDIR /usr/src/app

COPY ./requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY ./autoscaler.py ./

ENTRYPOINT ["python3", "-u", "autoscaler.py"]
