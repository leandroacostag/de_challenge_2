# Use the official Python base image
FROM python:3.9

# Create the app directory in the container
WORKDIR /app

# Copy the requirements.txt file to the container
COPY requirements.txt .

# Install the Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy the application code to the container
COPY app .

# Set the entrypoint command to run the Python application
CMD ["python", "main.py"]