# docker build command:
# docker build -t police-data-trust-transformer -f Dockerfile .
FROM python:3-alpine

WORKDIR /app/

ENV PYTHONPATH=/app/:$PYTHONPATH

COPY ./requirements.txt .
RUN pip install -r requirements.txt

COPY ./src .

WORKDIR /app/src

CMD [ "python3 main.py" ]
