FROM python:3.10-slim

WORKDIR /app

RUN apt-get update && apt-get install -y \
    gcc \
    g++ \
    make \
    libpq-dev \
    build-essential \
    --no-install-recommends \
    && rm -rf /var/lib/apt/lists/*

RUN python -m venv /app/user_venv

ENV PATH="/app/user_venv/bin:$PATH"

COPY . /app

COPY requirements.txt /app/requirements.txt
RUN pip install --upgrade pip && \
    pip install -r requirements.txt

EXPOSE 8765

CMD ["uvicorn", "app:app", "--host", "0.0.0.0", "--port", "8765"]
