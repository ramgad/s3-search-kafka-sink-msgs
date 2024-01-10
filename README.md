# Execute S3 Search against Kafka Messages sinked via s3 sink connector

## Install Pre-Requisites

Before running the python project, make sure you have the following pre-requisites installed:

### AWS-CLI

[Install AWS-CLI](https://docs.aws.amazon.com/cli/latest/userguide/getting-started-install.html)

### gimme-aws-creds

```bash
brew install gimme-aws-creds
```

### Python (3.11.6)

Follow the guide - [Install Python](https://docs.python-guide.org/starting/install3/osx/)

## Download the Pre-Requisites

Open a terminal and navigate to your project directory. Run the following commands to download and install the pre-requisites:

```bash
# Install virtualEnv for Python
pip install virtualenv

# Create a virtualEnv for Python
virtualenv <project-name>
for ex: virtualenv s3-kafka-messages

# Activate the virtualEnv
source <project-name>/bin/activate
for ex: source s3-kafka-messages/bin/activate

# Install required components
pip install -r requirements.txt
```

## Get gimme-aws-creds (Install if you do not have it)

Choose the Product account and save the credentials:

```bash
export AWS_PROFILE=Product
```

## Adjust Configurations

Adjust your inputs in the `config.ini` file based on your goals:

```ini
search_critirea         = entry1, entry2
search_date             = yyyy/mm/dd
search_hour             = 0-23
topics                  = topic1, topic2
enable_matched_content  = True/False
```

## Run the Program

1. Navigate to the shell where you have configured the virtualEnv.
2. Activate the virtualEnv if it's not already activated by running the following command:

    ```bash
    source <project-name>/bin/activate
    for ex: source s3-kafka-messages/bin/activate
    ```

3. Execute the Python program:

    ```bash
    python search-s3-kafka-messages.py
    ```

Now, your Python script will be executed.
