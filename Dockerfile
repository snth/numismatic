FROM python:3.6-alpine
WORKDIR /usr/src/app
COPY ./ /usr/src/app
RUN pip install -e .
WORKDIR numismatic
CMD coin
