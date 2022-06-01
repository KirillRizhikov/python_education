FROM python:3.10
WORKDIR /usr/src/app
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY insert_table.py ./

CMD [ "python", "./insert_table.py" ]