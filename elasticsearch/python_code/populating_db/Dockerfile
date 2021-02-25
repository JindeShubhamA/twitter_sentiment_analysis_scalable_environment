FROM python:3.6
WORKDIR /usr/src/app
COPY . .
RUN pip3 install --upgrade pip
RUN pip3 install elasticsearch
CMD ["import_in_elasticsearch.py"]
ENTRYPOINT [ "python3" ]
