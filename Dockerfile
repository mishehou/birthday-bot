FROM python:3.12-slim

WORKDIR /app

# Install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application
COPY app.py .
COPY templates/ templates/

# Data volume mount point
RUN mkdir -p /data

COPY gunicorn.conf.py .

EXPOSE 5000

CMD ["gunicorn", "app:app", "--config", "gunicorn.conf.py"]
