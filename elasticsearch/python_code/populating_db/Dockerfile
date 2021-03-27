FROM python:3.6
WORKDIR /usr/src/app
COPY . .
RUN pip3 install --upgrade pip
RUN pip3 install elasticsearch
RUN chmod +x ./run_scripts.sh
CMD ["sh", "./run_scripts.sh"]