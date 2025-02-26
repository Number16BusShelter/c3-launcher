FROM python:3.13-slim

# Set working directory
WORKDIR /app

# Install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Create a non-root user and group
RUN groupadd -r comput3 && useradd -r -g comput3 -m -s /bin/bash comput3

# Copy application file
COPY c3_launcher.py /app/

# Set ownership of the application files
RUN chown -R comput3:comput3 /app

# Switch to non-root user
USER comput3

# Run the application
ENTRYPOINT ["python", "c3_launcher.py"]
