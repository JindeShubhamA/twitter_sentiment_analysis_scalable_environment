# use the base image from our repository as a cache for python, java, and pip requirements
FROM richardswesterhof/mapreduce_base:latest
COPY . .
RUN pip3 install --upgrade pip
RUN pip3 install kafka-python
CMD python main.py