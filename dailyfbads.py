import requests
from facebook_business.api import FacebookAdsApi
from facebook_business.adobjects.adaccount import AdAccount
from facebook_business.adobjects.adsinsights import AdsInsights
from facebook_business.exceptions import FacebookRequestError
import gspread
from google.oauth2.service_account import Credentials
import time
import datetime
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Google Sheets API Setup
service_account_file = 'C:/Users/AAdmin/Desktop/Projects/Test/service_account.json'
scopes = ["https://www.googleapis.com/auth/spreadsheets"]

credentials = Credentials.from_service_account_file(service_account_file, scopes=scopes)
gc = gspread.authorize(credentials)

# Open the "Ad Account Id's" sheet
spreadsheet_id = '1YLBkV-jFCcHBpj6akpNuHDJcoNovEFglGaQ3cxJN7UQ'
ad_account_sheet = gc.open_by_key(spreadsheet_id).worksheet("Ad Account Id's")

# Fetch all data from the "Ad Account Id's" sheet
ad_accounts_data = ad_account_sheet.get_all_records()

# Filter active ad accounts (IDs should already be in the correct 'act_<account_id>' format)
active_ad_accounts = [
    row['Account Id']
    for row in ad_accounts_data
    if row['Status'].lower() == 'active'
]

# Open the "Fb Ads" sheet for data insertion
fb_ads_sheet = gc.open_by_key(spreadsheet_id).worksheet('Fb Ads')

# Function to convert a date to a Google Sheets serial number
def date_to_serial(date_str):
    epoch = datetime.datetime(1899, 12, 30)
    current_date = datetime.datetime.strptime(date_str, '%Y-%m-%d')
    date_serial = (current_date - epoch).days + (current_date - epoch).seconds / 86400
    return date_serial

# Function to refresh the access token
def refresh_access_token(app_id, app_secret, current_token):
    refresh_url = (
        f"https://graph.facebook.com/v20.0/oauth/access_token"
        f"?grant_type=fb_exchange_token"
        f"&client_id={app_id}"
        f"&client_secret={app_secret}"
        f"&fb_exchange_token={current_token}"
    )
    try:
        response = requests.get(refresh_url)
        response_data = response.json()
        if "access_token" in response_data:
            new_token = response_data['access_token']
            logging.info("Access token refreshed successfully.")
            return new_token
        else:
            logging.error(f"Failed to refresh access token: {response_data}")
            return current_token  # Use the current token if refresh fails
    except requests.exceptions.RequestException as e:
        logging.error(f"Error refreshing access token: {e}")
        return current_token  # Use the current token if an exception occurs

# Your Facebook API credentials
access_token = 'EAAG1wA3eIU0BOwJhNQDYvIkOvqSQQDwzaAMiZBepSvalnyAlK4rOH55PqJd9k2jzvD8pfosx4d70JGD2xk3CTCZBL074pZCc0453iW8csDVClEuJA40ATjIsFdCZAAGjkpP1Hsd0hK7qgVyi9I3fDYroKbgnooSpveVGrRSZCwbNJ6yei5SFhKqfS4UCJTgaZB'
app_secret = '28a4d12119645fe1dd56a11eb5f53f0e'
app_id = '481311447720269'

# Refresh the access token and update it
access_token = refresh_access_token(app_id, app_secret, access_token)

# Initialize the Facebook API with the refreshed token
FacebookAdsApi.init(app_id, app_secret, access_token)

# Calculate yesterday's date
yesterday = datetime.datetime.now() - datetime.timedelta(1)
formatted_date = yesterday.strftime('%Y-%m-%d')

# Define the date range for only yesterday
start_date = formatted_date
end_date = formatted_date

# Define the fields you want to fetch
fields = [
    AdsInsights.Field.account_id,
    AdsInsights.Field.account_name,
    AdsInsights.Field.campaign_id,
    AdsInsights.Field.campaign_name,
    AdsInsights.Field.date_start,
    AdsInsights.Field.impressions,
    AdsInsights.Field.clicks,
    AdsInsights.Field.cpc,
    AdsInsights.Field.ctr,
    AdsInsights.Field.spend,
    AdsInsights.Field.reach,
    AdsInsights.Field.frequency,
    AdsInsights.Field.unique_clicks,
    AdsInsights.Field.actions
]

# Function to handle API rate limits with exponential backoff
def handle_rate_limit(func):
    def wrapper(*args, **kwargs):
        delay = 60  # Start with a 60-second delay
        for attempt in range(5):  # Try up to 5 times
            try:
                return func(*args, **kwargs)
            except gspread.exceptions.APIError as e:
                if "quota" in str(e).lower():
                    logging.warning(f"Google Sheets rate limit exceeded. Waiting {delay} seconds before retrying...")
                    time.sleep(delay)
                    delay *= 2  # Exponential backoff
                else:
                    raise e
            except FacebookRequestError as e:
                if e.api_error_code() in [4, 17, 32, 613]:  # Common rate limit error codes
                    logging.warning(f"Facebook API rate limit reached. Waiting {delay} seconds before retrying...")
                    time.sleep(delay)
                    delay *= 2  # Exponential backoff
                else:
                    raise e
    return wrapper

@handle_rate_limit
def fetch_facebook_insights(ad_account, fields, params):
    return ad_account.get_insights(fields=fields, params=params)

@handle_rate_limit
def batch_append_rows(sheet, rows):
    # Append rows as a batch at the bottom of the sheet
    sheet.append_rows(rows, value_input_option='USER_ENTERED')

# Process each active ad account one at a time
for ad_account_id in active_ad_accounts:
    try:
        logging.info(f"Processing Ad Account: {ad_account_id}")
        ad_account = AdAccount(ad_account_id)
        
        # Fetch data
        insights = fetch_facebook_insights(ad_account, fields, {
            'time_range': {'since': start_date, 'until': end_date},
            'level': 'campaign',
            'filtering': [],
            'breakdowns': [],
            'time_increment': 1,
        })

        rows = []
        for insight in insights:
            campaign_name = insight.get('campaign_name')

            # Check if the campaign name contains "IVS"
            if "IVS" in campaign_name:
                account_id = 'act_' + str(insight.get('account_id'))
                account_name = insight.get('account_name')
                campaign_id = insight.get('campaign_id')
                date_start_serial = date_to_serial(insight.get('date_start'))

                impressions = int(insight.get('impressions', 0))
                clicks = int(insight.get('clicks', 0))
                cpc = float(insight.get('cpc', 0))
                ctr = float(insight.get('ctr', 0))
                spend = float(insight.get('spend', 0))
                reach = int(insight.get('reach', 0))
                frequency = float(insight.get('frequency', 0))
                unique_clicks = int(insight.get('unique_clicks', 0))

                actions = insight.get('actions', [])
                action_map = {action['action_type']: int(action['value']) for action in actions}

                onsite_conversion_messaging_first_reply = action_map.get('onsite_conversion.messaging_first_reply', 0)
                landing_page_view = action_map.get('landing_page_view', 0)
                onsite_conversion_post_save = action_map.get('onsite_conversion.post_save', 0)
                comment = action_map.get('comment', 0)
                page_engagement = action_map.get('page_engagement', 0)
                post_engagement = action_map.get('post_engagement', 0)
                lead = action_map.get('lead', 0)
                onsite_web_lead = action_map.get('onsite_web_lead', 0)
                post = action_map.get('post', 0)
                like = action_map.get('like', 0)
                offsite_conversion_fb_pixel_lead = action_map.get('offsite_conversion.fb_pixel_lead', 0)
                onsite_conversion_messaging_conversation_started_7d = action_map.get('onsite_conversion.messaging_conversation_started_7d', 0)
                onsite_conversion_lead_grouped = action_map.get('onsite_conversion.lead_grouped', 0)
                post_reaction = action_map.get('post_reaction', 0)
                link_click = action_map.get('link_click', 0)

                row = [
                    account_id,
                    account_name,
                    campaign_id,
                    campaign_name,
                    date_start_serial,
                    impressions,
                    clicks,
                    cpc,
                    ctr,
                    spend,
                    reach,
                    frequency,
                    unique_clicks,
                    onsite_conversion_messaging_first_reply,
                    landing_page_view,
                    onsite_conversion_post_save,
                    comment,
                    page_engagement,
                    post_engagement,
                    lead,
                    onsite_web_lead,
                    post,
                    like,
                    offsite_conversion_fb_pixel_lead,
                    onsite_conversion_messaging_conversation_started_7d,
                    onsite_conversion_lead_grouped,
                    post_reaction,
                    link_click
                ]
                rows.append(row)

        # Insert data into Google Sheet as a batch
        if rows:
            logging.info(f"Inserting data for Ad Account {ad_account_id}")
            batch_append_rows(fb_ads_sheet, rows)
            logging.info(f"Data for Ad Account {ad_account_id} inserted successfully.")
        
        time.sleep(5)  # 5-second delay between processing each ad account

    except gspread.exceptions.APIError as e:
        logging.error(f"APIError for Ad Account {ad_account_id}: {e}")
    except FacebookRequestError as e:
        if e.api_error_code() == 100:
             logging.warning(f"No access or invalid ad account {ad_account_id}. Skipping...")
        else:
            logging.error(f"Failed to fetch data for Ad Account {ad_account_id}: {e}")
    except Exception as e:
        logging.error(f"Unexpected error for Ad Account {ad_account_id}: {e}")

logging.info("Data insertion process completed.")
