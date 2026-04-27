# Garmin Data Pipeline

The Garmin data comes distributed in different directories and with very coded names which makes the insightful data very difficult to access and handle. The aim of this pipeline is to help extract the Garmin data and prepare it for further post-process.

## Files
`create_sleep_dataset.py` - Extracts and processes the sleep data from the given data folder.

## Setup
- Clone the repository in your desired directory.
- Install [uv](https://docs.astral.sh/uv/) if you don't have it. 
- Create the virtual environment and install dependencies: `uv sync`.
- Access the virtual environment: `source .venv/bin/activate`
- Create a `data` directory and add your Garmin export there. 
- Create **sleep_data** by running the script called *create_sleep_dataset* with the following command:
```
python create_sleep_dataset.py
```

## Contribution
Pull requests and issues are welcome.