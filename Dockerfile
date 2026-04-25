# base Docker image that we will build on
FROM python:3.11-slim

# Copy uv binary from official uv image (multi-stage build pattern)
COPY --from=ghcr.io/astral-sh/uv:latest /uv /bin/

# # set up our image by installing prerequisites; pandas in this case
# RUN pip install pandas pyarrow

# set up the working directory inside the container
WORKDIR /app

# Add virtual environment to PATH so we can use installed packages
ENV PATH="/app/.venv/bin:$PATH"
ENV PYTHONPATH="/app"

# Copy dependency files first (better layer caching)
COPY "pyproject.toml" "uv.lock" ./ 
COPY app/ .

# Install dependencies from lock file (ensures reproducible builds)
RUN uv sync --locked

# define what to do first when the container runs
# in this example, we will just run the script
ENV PYTHONUNBUFFERED=1
