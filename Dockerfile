FROM python:3.11-slim

WORKDIR /app

COPY proxy/requirements.txt ./requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

COPY src ./src
COPY proxy ./proxy
COPY config/models.json ./config/models.json

ENV PYTHONUTF8=1
EXPOSE 7860

CMD ["python", "proxy/main.py"]
