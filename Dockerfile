FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt ./requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

COPY main.py ./main.py
COPY static ./static
COPY src ./src
COPY config.json.example ./config.json

ENV PYTHONUTF8=1
EXPOSE 7860

CMD ["python", "main.py"]
