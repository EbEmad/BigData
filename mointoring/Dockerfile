FROM ghcr.io/astral-sh/uv:0.6.14-python3.10-bookworm

WORKDIR /src

# Install additional system dependencies for lxml and other packages
RUN apt-get update && apt-get install -y \
    build-essential \
    libavif-dev pkg-config \
    libjpeg-dev \
    gcc unzip zip \
    python3-dev \
    libxml2-dev \
    libxslt1-dev \
    libffi-dev \
    curl \
    && rm -rf /var/lib/apt/lists/*

COPY ./requirements.txt .


RUN uv pip install -r requirements.txt --system

COPY  src/ .

CMD [ "uvicorn","app:app", "--host", "0.0.0.0", "--port", "8000","--workers","4" ]