# Use the official Google Cloud SDK image as the base image
FROM google/cloud-sdk:latest

# Set a working directory in the container
WORKDIR /app

# Install uv package manager
RUN pip install uv
# RUN curl -LsSf https://astral.sh/uv/install.sh | sh
# RUN source $HOME/.local/bin/env

# Copy the rest of the application code
COPY . .

# Specify the command to run the application
CMD ["uv", "run", "benchmarks.py"]

