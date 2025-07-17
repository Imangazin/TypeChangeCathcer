import requests
from requests.auth import HTTPBasicAuth
import os
import dotenv
import pandas as pd
import re
from datetime import datetime, timedelta, timezone
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import zipfile
from logger_config import logger

# Function to check if the string matches the pattern and length
def matches_pattern(s):
    # Define the regex pattern
    pattern = r'\d{4}-[A-Z]{2}-D\d{2}-[A-Z]{4}-\dP\d{2}-S\d{2}-[A-Z]{3}'
    # Will check only section code that has lenght of 29
    pattern_length = 29
    if pd.isna(s) or s == "":
        return False
    return bool(re.fullmatch(pattern, s)) and len(s) == pattern_length

# Function to remove the last 3 characters from the string
def remove_last_three_chars(s):
    if pd.isna(s) or s == "":
        return s
    return s[:-4]

# Get recent duplicate sections 
def find_duplicates_and_email():
    # Read the CSV file into a DataFrame
    df = pd.read_csv('files/OrganizationalUnits.csv')

    # Parse CreatedDate and drop invalid rows
    df['CreatedDate'] = pd.to_datetime(df['CreatedDate'], errors='coerce', utc=True)
    filtered_df = df[df['Code'].apply(matches_pattern)].copy()
    filtered_df = filtered_df.dropna(subset=['CreatedDate'])

    # Debug logging
    print("Filtered rows:", len(filtered_df))
    print("NaT in CreatedDate:", filtered_df['CreatedDate'].isna().sum())
    print(filtered_df[['Code', 'CreatedDate']].head(5))

    if filtered_df.empty:
        logger.warning("Filtered DataFrame is empty after applying pattern and date cleanup.")
        return

    # Remove duplicates based on 'Code', keeping the entry with the most recent 'CreatedDate'.
    # This is to avoid the merged courses.
    filtered_df = filtered_df.loc[
        filtered_df.groupby('Code')['CreatedDate'].idxmax()
    ]

    # Apply the function to remove the last 3 characters from the filtered column
    filtered_df.loc[:,'ModifiedCode'] = filtered_df['Code'].apply(remove_last_three_chars)

    # Find duplicates in the 'modified_column' column
    duplicates_df = filtered_df[filtered_df.duplicated(subset=['ModifiedCode'], keep=False)].copy()

    # Convert 'CreatedDate' column to datetime format, ensuring it's timezone-aware (UTC)
    duplicates_df.loc[:,'CreatedDate'] = pd.to_datetime(duplicates_df['CreatedDate'], utc=True)

    # Calculate the date 7 days ago from current UTC time
    seven_days_ago = datetime.now(timezone.utc) - timedelta(days=7)

    # Filter duplicates_df to include only rows where at least one duplicate was created in the last 7 days
    recent_duplicates_df = duplicates_df[
        duplicates_df.groupby('ModifiedCode')['CreatedDate'].transform(lambda x: x.max() >= seven_days_ago)
    ]

    recent_duplicates_df.to_csv('files/recent_duplicates_output.csv', index=False)

    # Check if recent duplicates were found
    if not recent_duplicates_df.empty:
        message = ""
        for index, row in recent_duplicates_df.iterrows():
            message += f"{config['bspace_url']}/d2l/home/{row['OrgUnitId']} - {row['Code']}" + "<br>"
        
        send_email(message, config["send_to"], config["from"])
    else:
        logger.info("No duplicate sections found.")


# send an email to edtech and nimangazin
def send_email(sections, to_email, from_email):
    # Define email components
    subject = "Duplicate Section Information Detected"
    html_body = f"""
    <html>
    <body>
        <h3>Greetings from Section Type Change Catcher Script,</h3>
        <p>This is an automatic email message. Please see the duplicate section information below:</p>
        <p><mark>{sections}</mark></p>
        <aside>
            Note: This script will be running every Thursday at 9:00 am.
        </aside>        
        <p>Thank you.</p>
        <p>Cheers,</p>
    </body>
    </html>
    """  # The HTML content of the email

    # Create the MIME message
    msg = MIMEMultipart('alternative')
    msg['From'] = from_email
    msg['To'] = to_email
    msg['Subject'] = subject

    # Attach the HTML body to the message
    msg.attach(MIMEText(html_body, 'html'))

    try:
        with os.popen(f"sendmail -t", "w") as p:
            p.write(msg.as_string())
        print("Email sent successfully.")
    except Exception as e:
        print("Error sending email:", str(e))



# Get access token and refresh token
def trade_in_refresh_token(config):
    response = requests.post(
        f'{config["auth_service"]}/core/connect/token',
        # Content-Type 'application/x-www-form-urlencoded'
        data={
            'grant_type': 'refresh_token',
            'refresh_token': config['refresh_token'],
            'scope': config['scope']
        },
        auth=HTTPBasicAuth(config['client_id'], config['client_secret'])
    )
    return response.json()

# Valence call
def call_with_auth(method, endpoint, access_token, data=None):
    try:
        headers = {'Authorization': f'Bearer {access_token}'}
        method = method.upper()

        if method == 'GET':
            response = requests.get(endpoint, headers=headers)
        elif method == 'POST':
            response = requests.post(endpoint, headers=headers, json=data)
        else:
            raise ValueError("Unsupported HTTP method. Use 'GET' or 'POST'.")
        
        response.raise_for_status()
        return response
    except Exception as e:
        return None

# downloads Data Hub report
def get_report(config, file_path, access_token):
    #get the link
    extracts = call_with_auth("GET", f"{config['bspace_url']}/d2l/api/lp/1.47/datasets/bds/{config['schema_id']}/plugins/{config['plugin_id']}/extracts", access_token)
    if extracts!=None:
        # Download the data set
        last_full_data = call_with_auth("GET", extracts.json()['Objects'][0]["DownloadLink"], access_token)
        with open(file_path, 'wb') as file:
            file.write(last_full_data.content)

# Unzipping the file
def unzip_file(zip_file_path, extract_to_folder):
    # Check if the zip file exists
    if not os.path.exists(zip_file_path):
        return 
    # Check if the output folder exists, if not, create it
    if not os.path.exists(extract_to_folder):
        os.makedirs(extract_to_folder) 
    # Unzipping the file
    with zipfile.ZipFile(zip_file_path, 'r') as zip_ref:
        zip_ref.extractall(extract_to_folder)



# Main code

logger.info("Started...")

#Read api keys, tokens, etc. fron .env file to local dictionary 
dotenv_file = dotenv.find_dotenv()
dotenv.load_dotenv(dotenv_file)

config = {
    "bspace_url": os.environ["bspace_url"],
    "auth_service": os.environ["auth_service"],
    "client_id": os.environ["client_id"],
    "client_secret": os.environ["client_secret"],
    "scope": os.environ["scope"],
    "schema_id": os.environ["schema_id"],
    "plugin_id": os.environ["plugin_id"],
    "refresh_token": os.environ["refresh_token"],
    "send_to": os.environ["send_to"],
    "from": os.environ["from"]
}

# Get access and refresh tokens and update refresh token in the .env file
auth_tokens = trade_in_refresh_token(config)
os.environ["refresh_token"] = auth_tokens['refresh_token']
dotenv.set_key(dotenv_file, "refresh_token", os.environ["refresh_token"])

logger.info("Tokens obtained and refreshed")

# Downloading the zip report
get_report(config, "files/org_units.zip",auth_tokens['access_token'])

#Unzip the report
unzip_file("files/org_units.zip", "files")

# Find duplicates and email
find_duplicates_and_email()
