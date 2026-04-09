FROM python:3.12-alpine
ENV PYTHONUNBUFFERED=1
WORKDIR /app
COPY app.py .
EXPOSE 8000
CMD ["python3", "app.py"]
