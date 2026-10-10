FROM python:3.10

WORKDIR /usr/src/app

COPY ./requirements.txt ./
RUN pip install -r requirements.txt

COPY ./consumer.py ./
COPY ./notifier.py ./

VOLUME ["/usr/src/app/auth"]

ENTRYPOINT ["python3", "-u"]
