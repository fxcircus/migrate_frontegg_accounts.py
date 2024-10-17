# Migration Script Instructions
Script that migrates data from one Frontegg account to another.

## Configuration Steps
1. Ensure you have Python 3 and pip installed. Install Dependencies:
```
pip install pandas requests python-dotenv
```
2. Set Up Frontegg Credentials
Open the script file and provide API credentials for both the source and destination Frontegg accounts:
```
CLIENT_ID_1 = ''  # Source account Client ID
API_KEY_1 = ''    # Source account API Key

CLIENT_ID_2 = ''  # Destination account Client ID
API_KEY_2 = ''    # Destination account API Key
```

3. Prepare Data Files
Ensure that an `account_data` directory exists in the same directory as the script, containing the following CSV files:

- user_details_prod.csv
- user_passwords_prod.csv


## Running the Script
Navigate to the directory where the script is located.
Run the script:
```
python migrate_frontegg_accounts.py
```



The log file `log.txt` will be created in the same directory as the script.
