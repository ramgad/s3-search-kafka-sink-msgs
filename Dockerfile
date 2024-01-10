# Use an official Python runtime as a parent image
FROM python:3.11.6

# Set the working directory to /app
WORKDIR /app

# Copy the current directory contents into the container at /app
COPY . /app

# Install any needed packages specified in requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Define environment variable
ENV AWS_ACCESS_KEY_ID=$AWS_ACCESS_KEY_ID
ENV AWS_SECRET_ACCESS_KEY=$AWS_SECRET_ACCESS_KEY
ENV AWS_SESSION_TOKEN=$AWS_SESSION_TOKEN
ENV AWS_DEFAULT_REGION=$AWS_DEFAULT_REGION
ENV NUM_THREADS=512


# Run script.py when the container launches
#CMD ["python", "thread2.py"]
CMD ["python", "search-s3-kafka-messages.py"]
#CMD ["python", "threads.py"]
