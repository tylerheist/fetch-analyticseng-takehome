FROM ghcr.io/astral-sh/uv:python3.12-bookworm
COPY requirements.txt .
RUN uv pip install -r requirements.txt --system