FROM python:3.10

WORKDIR /usr/src/app

COPY ./requirements.txt ./
RUN pip install -r requirements.txt

COPY ./api.py ./

EXPOSE 3003

ENTRYPOINT ["python3", "-u", "api.py"]
