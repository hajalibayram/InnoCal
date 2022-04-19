import pickle
import os
import datetime
from time import time, sleep
from dateutil.tz import tzlocal
import numpy as np
import pandas as pd
import logging


from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request


# If modifying these scopes, delete the file token.pickle.
SCOPES = ['https://www.googleapis.com/auth/calendar']

CREDENTIALS_FILE = 'credentials.json'
CREATOR_EMAIL = 'bayramov@its-ictpiemonte.it'

def setup_log():
    start_time = datetime.datetime.now()
    logs_folder = "logs/"
    logs_file=f"{logs_folder}/{start_time.strftime('%Y-%m-%d_%H-%M')}.log"

    if os.path.exists(logs_folder):
        print(FileExistsError(f"{logs_folder} already exists!"))
    else:
        os.makedirs(logs_folder, exist_ok=True)
    # base_formatter = logging.Formatter('%(asctime)s   %(message)s', "%Y-%m-%d %H:%M")
    logger = logging.getLogger('')
    logger.setLevel(logging.DEBUG)

    open(logs_file, 'a').close()

    logging.basicConfig(filename=logs_file, filemode='w', format='%(name)s - %(levelname)s - %(message)s')
    logging.info(f"Log file: {start_time.strftime('%Y-%m-%d_%H-%M')} \n \n ")


def getCalendarService():
    creds = None
    # The file token.pickle stores the user's access and refresh tokens, and is
    # created automatically when the authorization flow completes for the first
    # time.
    try:
        if os.path.exists('token.pickle'):
            with open('token.pickle', 'rb') as token:
                creds = pickle.load(token)
    # If there are no (valid) credentials available, let the user log in.
        if not creds or not creds.valid:
            if creds and creds.expired and creds.refresh_token:
                creds.refresh(Request())
            else:
                flow = InstalledAppFlow.from_client_secrets_file(
                CREDENTIALS_FILE, SCOPES)
                creds = flow.run_local_server(port=0)

        # Save the credentials for the next run
            with open('token.pickle', 'wb') as token:
                pickle.dump(creds, token)

        service = build('calendar', 'v3', credentials=creds)
    except Exception as e:
        logging.error(e)
    
    return service


def listCalendarsFromGoogle(service):
    try:
        # Call the Calendar API
        cals = {}
        logging.info('Getting list of calendars >>>> ')
        print('Getting list of calendars >>>> ')
        calendars_result = service.calendarList().list().execute()

        calendars = calendars_result.get('items', [])

        if not calendars:
            logging.info('No calendars found.')
            print('No calendars found.')
        for calendar in calendars:
            summary = calendar['summary']
            id = calendar['id']
            primary = "Primary" if calendar.get('primary') else ""
            logging.info("%s\t%s\t%s" % (summary, id, primary))
    #         print("%s\t%s\t%s" % (summary, id, primary))
            cals[summary] = id
        logging.info(f'{len(calendars)} calendars found!\n')
        print(f'{len(calendars)} calendars found!\n')
    except Exception as e:
        logging.error(e)
    return cals



def addEventToGoogle(df, service, locali = None, attempt = 0, deadline = 10, waittime = 61):
    c_attempt = attempt
    try:
        calsDict = listCalendarsFromGoogle(service)
        timezone = 'Europe/Rome'
        if locali == None:
            locali = df['Locale'].unique()

        for l in locali:
            df_t = df[df['Locale'] == l]
        
            df_t = df_t.sort_values(by=['Inizio'])

            # get events from yesterday and further 
            df_t = df_t[df_t['Inizio'] > (datetime.datetime.utcnow() - datetime.timedelta(days=1)).isoformat()]
            
            df_t['Inizio'] = [t.replace(tzinfo = tzlocal()).isoformat() for t in df_t['Inizio']]
            df_t['Fine'] = [t.replace(tzinfo = tzlocal()).isoformat() for t in df_t['Fine']]
            
            
            calId = calsDict.get(l) 
            if calId is None:
                calendar = {
                    'summary': l,
                    'timeZone': timezone
                }
                cal = service.calendars().insert(body=calendar).execute()   
                calId = cal['id']
                logging.info(f"Calendar {l} is added")
                print(f"Calendar {l} is added")
            else:
                logging.info(f"Calendar {l} already exists")
                print(f"Calendar {l} already exists")
            
            events_all = []
            page_token = None
            while True:
                events_t = service.events().list(calendarId= calId, pageToken=page_token).execute()
                events_all.extend(events_t['items'])

                page_token = events_t.get('nextPageToken')
                if not page_token:
                    break
    
            
            stCal = [d['start']['dateTime'] for d in events_all if d['creator']['email'] == CREATOR_EMAIL and d['start']['dateTime'] >(datetime.datetime.utcnow() - datetime.timedelta(days=1)).isoformat()]
            enCal = [d['end']['dateTime'] for d in events_all if d['creator']['email'] == CREATOR_EMAIL and d['start']['dateTime'] >(datetime.datetime.utcnow() - datetime.timedelta(days=1)).isoformat()]
            
            
            df_cal = pd.DataFrame({'Inizio':stCal, 'Fine':enCal})
            df_extra = pd.merge(df_t, df_cal, left_on = ['Inizio', 'Fine'], right_on = ['Inizio', 'Fine'], how="left", indicator=True).query('_merge=="left_only"')
            df_deleted = pd.merge(df_t, df_cal, left_on = ['Inizio', 'Fine'], right_on = ['Inizio', 'Fine'], how="right", indicator=True).query('_merge=="right_only"')

            l_inizio = df_extra['Inizio'].tolist()
            l_fine = df_extra['Fine'].tolist()
            l_sede = df_extra['Db'].tolist()

            logging.info(f'{len(l_inizio)} new events' )
            logging.info(f'{len(df_cal["Inizio"])} old events' )
            logging.info(f'{len(df_t["Inizio"])} total events from today \n\n' )
            print(f'{len(l_inizio)} new events' )
            print(f'{len(df_cal["Inizio"])} old events' )
            print(f'{len(df_t["Inizio"])} total events \n\n' )
            
            dl_inizio = df_deleted['Inizio'].tolist()
            dl_fine = df_deleted['Fine'].tolist()
            
            
            for d in range(len(l_inizio)):
                
                logging.info(f'{d+1}: Adding event {l}, at {l_inizio[d]}')
                print(f'{d+1}: Adding event {l}, at {l_inizio[d]}')
                event_result = service.events().insert(calendarId=calId,
                body={
                "summary": l,
                "description": l_sede[d],
                "start": {"dateTime": l_inizio[d], "timeZone": timezone},
                "end": {"dateTime": l_fine[d], "timeZone": timezone},
                }
                ).execute()
            
            for d in range(len(dl_inizio)):
                logging.info(f'{d+1}: Deleting event {l}, at {dl_inizio[d]}')
                print(f'{d+1}: Deleting event {l}, at {dl_inizio[d]}')
    #            print(service.events().list(calendarId=calId, timeMin=dl_inizio[d], timeMax=dl_fine[d]).execute())
                eventId = service.events().list(calendarId=calId, timeMin=dl_inizio[d], timeMax=dl_fine[d]).execute()['items'][0]['id']
                service.events().delete(calendarId =calId, eventId = eventId).execute()
    except Exception as e:        
        logging.error(e)
        if '403' in str(e):
            logging.error('403')
        if c_attempt < deadline:
            logging.error(f'Reattempt #{c_attempt+1}')
            print(f'Reattempt #{c_attempt+1}')
            logging.error(f'Waiting for {waittime}s')
            print(f'Waiting for {waittime}s')
            sleep((waittime)+np.random.random())
            addEventToGoogle(df, service, locali = locali, attempt = c_attempt+1)
        else:
            logging.error(f'Reattempt finished')
            print(f'Reattempt finished')
            
            quit()
        # print(e)
    # finally:
    #     return True
