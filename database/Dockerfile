FROM arm64v8/python:3.7-alpine

WORKDIR /usr/src/database

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

CMD [ "python", "-u", "app.py" ]
