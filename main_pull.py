import os
import time
import pandas as pd
import json
from pytrends.request import TrendReq
from datetime import date, datetime, timedelta
from dateutil.relativedelta import *
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from gspread_dataframe import get_as_dataframe, set_with_dataframe

def main():

    short_date_range = '2020-04-05 2020-04-13'
    google_sheet = "Untitled Document"
    gsheet_tab = "Sheet1"
    cred_location = "/Desktop/"
    cred_file = "creds.json"
    scopes = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']

    pytrends = TrendReq(hl='en-US', tz=360, timeout=30)

    searches = pull_keywords(cred_location, cred_file, scopes)

    dataframe_short = pullCustomDataRange(pytrends, searches, short_date_range, 'file3')

    sheet_obj = access_worksheet(google_sheet, gsheet_tab, cred_location, cred_file, scopes)

    #incremental data pull
    next_row = next_avail_row(sheet_obj)
    print("NEXT ROW:", next_row)
    next_col = next_avail_col(sheet_obj)
    print("NEXT COL:", next_col)
    set_with_dataframe(sheet_obj, dataframe_short, row=next_row, include_column_header=True)


def pull_keywords(cred_location, cred_file, scopes):
    cred_path = os.path.expanduser("~" + cred_location + cred_file)
    creds = ServiceAccountCredentials.from_json_keyfile_name(cred_path, scopes)
    client = gspread.authorize(creds)
    sheet = client.open_by_url('https://docs.google.com/spreadsheets/d/1y_I4Z1Iq7y9gy4b6L4Aet0AdLvv58pa8o85q7C1crdo/edit#gid=0')
    worksheet = sheet.get_worksheet(0)
    mapping_tab_data = worksheet.get_all_values()
    df_header = mapping_tab_data[0] # columns
    df = pd.DataFrame.from_records(mapping_tab_data[1:], columns=df_header)
    searches = df['Keyword'].tolist()

    return searches


def pullCustomDataRange(pytrends, searches, customDate, file_name):

    keywordGroups = list(zip(*[iter(searches)] * 1))
    keywordGroups = [list(x) for x in keywordGroups]


    dict = {}
    i = 1

    for term in keywordGroups:
        pytrends.build_payload(trending, timeframe=customDate, geo='US')
        dicti[i] = pytrends.interest_over_time()
        print(dict[i])
        time.sleep(5)
        i += 1

    finalDf = pd.concat(dict, axis=1)
    finalDf.columns = finalDf.columns.droplevel(0)
    finalDf = finalDf.drop('isPartial', axis=1)
    finalDf.reset_index(level=0, inplace=True)
    finalDf = finalDf.rename(columns={'index':'date'})
    sheetDf = pd.melt(finalDf, id_vars=['date'], var_name= "Search Term", value_name="Index")

    sheetDf.to_csv(file_name+'.csv')

    return sheetDf

# GSHEET FUNCTIONS
def access_worksheet(google_sheet, gsheet_tab, cred_location, cred_file, scopes):
    cred_path = os.path.expanduser("~" + cred_location + cred_file)
    creds = ServiceAccountCredentials.from_json_keyfile_name(cred_path, scopes)
    client = gspread.authorize(creds)

    gwb = client.open(google_sheet)
    return gwb.worksheet(gsheet_tab)


def next_avail_row(worksheet):
    str_list = list(filter(None, worksheet.col_values(1)))
    return len(str_list) + 1  


def next_avail_col(worksheet):
    str_list = list(filter(None, worksheet.row_values(1)))
    return len(str_list) + 1 

if __name__ == '__main__':
    main()
