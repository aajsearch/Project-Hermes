FROM python:3.12-slim

WORKDIR /app
COPY robinhood_agentic/requirements-app.txt /tmp/requirements-app.txt
RUN pip install --no-cache-dir -r /tmp/requirements-app.txt

COPY robinhood_agentic /app/robinhood_agentic

ENV PYTHONUNBUFFERED=1
# Set COMMAND_CENTER_PIN in production
EXPOSE 8787

HEALTHCHECK --interval=30s --timeout=5s --retries=3 \
  CMD python -c "import urllib.request; urllib.request.urlopen('http://127.0.0.1:8787/api/health')"

CMD ["python", "-m", "robinhood_agentic.app", "--host", "0.0.0.0", "--port", "8787"]
