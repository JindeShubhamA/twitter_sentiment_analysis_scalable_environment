# use the base image from our repository as a cache for python, java, and pip requirements
FROM richardswesterhof/scalable_computing:mapreduce_base
COPY . .

RUN chmod +x "./run_driver.sh"

#CMD python main.py
CMD ["sh", "./run_driver.sh"]