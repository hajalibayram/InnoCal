import pickle
import os
import datetime
from time import time, sleep
from turtle import st
from dateutil.tz import tzlocal
import numpy as np
import pandas as pd
import logging
import pyodbc


from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request


# If modifying these scopes, delete the file token.pickle.
SCOPES = ['https://www.googleapis.com/auth/calendar']

CREDENTIALS_FILE = 'credentials.json'
CREATOR_EMAIL = 'calendar@its-ictpiemonte.it'

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


def createCalendars(df, service, locali, cals_dict, attempt = 0, deadline = 10, waittime = 61):
    c_attempt = attempt
    timezone = 'Europe/Rome'

    try:
        for l in locali:
            if l in cals_dict.keys():
                print(f'{l} is in G-Calendar!')
                continue
            print(f'Creating {l} calendar...')
            df_l = df[df['locale']==l]
            # print(df_l['Pin'], f"{df_l['Db']};{df_l['Sede']}")
            calendar = {
                'summary': l,
                'timeZone': timezone,
                'location': str(df_l.iloc[0]['Pin']),
                'description': f"{df_l.iloc[0]['Db']};{df_l.iloc[0]['Sede']}"
            }

            created_calendar = service.calendars().insert(body=calendar).execute()
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
            createCalendars(df, service, locali = locali, cals_dict=cals_dict, attempt = c_attempt+1, waittime=(c_attempt+1)*1.5)
        else:
            logging.error(f'Reattempt finished')
            print(f'Reattempt finished')
            
            
        # print(e)
    finally:
        return True
    return True


def addEventToGoogle(df, service, cals_dict, locali = None, deadline_days = 30, attempt = 0, deadline = 10, waittime = 61):
    c_attempt = attempt
    timezone = 'Europe/Rome'
    try:
        # if locali == None:
        #     locali = df['Locale'].unique()
        for l in locali:
            df_t = df[df['Locale'] == l]
        
            df_t = df_t.sort_values(by=['Inizio'])

            # get events from today and 1 month further 
            df_t = df_t[(df_t['Inizio'] > datetime.datetime.utcnow().isoformat()) & (df_t['Inizio'] < (datetime.datetime.utcnow() + datetime.timedelta(days=deadline_days)).isoformat())]
            
            df_t['Inizio'] = [t.replace(tzinfo = tzlocal()).isoformat() for t in df_t['Inizio']]
            df_t['Fine'] = [t.replace(tzinfo = tzlocal()).isoformat() for t in df_t['Fine']]
            
            calId = cals_dict.get(l) 
            # if calId is None:
            #     calendar = {
            #         'summary': l,
            #         'timeZone': timezone
            #     }
            #     cal = service.calendars().insert(body=calendar).execute()   
            #     calId = cal['id']
            #     logging.info(f"Calendar {l} is added")
            #     print(f"Calendar {l} is added")
            # else:
                # logging.info(f"Calendar {l} already exists")
                # print(f"Calendar {l} already exists")
            
            events_all = []
            page_token = None
            while True:
                events_t = service.events().list(calendarId= calId, pageToken=page_token).execute()
                events_all.extend(events_t['items'])

                page_token = events_t.get('nextPageToken')
                if not page_token:
                    break
            stCal = []
            enCal = []
            if (d['creator']['email'] == CREATOR_EMAIL) & (d['start']['dateTime'] > datetime.datetime.utcnow().isoformat()) & (d['start']['dateTime'] < (datetime.datetime.utcnow() + datetime.timedelta(days=deadline_days)).isoformat()):
                stCal.append(d['start']['dateTime'])
                enCal.append(d['end']['dateTime'])
                            
            
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
            
            
        # print(e)
    finally:
        return True


def addEventToInnovaplan(df, service, cals_dict, locali, conn, attempt = 0, deadline = 10, waittime = 61):
    c_attempt = attempt
    timezone = 'Europe/Rome'
    deadline_days = 30
    try:
        for l in locali:
            print(l)
            try:
                # Get calendar object
                calId = cals_dict[l]
                cal = service.calendars().get(calendarId = calId).execute()
                # Get all events in calendar l
                events_all = []
                page_token = None
                while True:
                    events_t = service.events().list(calendarId= calId, pageToken=page_token).execute()
                    events_all.extend(events_t['items'])

                    page_token = events_t.get('nextPageToken')
                    if not page_token:
                        break
                # print('Events \n',events_all)
                # print('Calendar\n', cal)
                # print('\n\n\n')
                
                df_t = df[df['Locale'] == l]
                df_t = df_t.sort_values(by=['Inizio'])
                # print(df_t)

                # get events from today and 1 month further 
                df_t = df_t[(df_t['Inizio'] > datetime.datetime.utcnow().isoformat()) & (df_t['Inizio'] < (datetime.datetime.utcnow() + datetime.timedelta(days=deadline_days)).isoformat())]

                df_t['Inizio'] = [t.replace(tzinfo = tzlocal()).isoformat() for t in df_t['Inizio']]
                df_t['Fine'] = [t.replace(tzinfo = tzlocal()).isoformat() for t in df_t['Fine']]
                # print(cals_dict[l])
                
                # print(events_all)
                # for e in events_all:
                #     if e['creator']['email'] != CREATOR_EMAIL:
                #         print(e)

                stCal_out = []
                enCal_out = []
                summaryCal_out = []
                creatorCal_out = []
                idCal_out = []
                
                for d in events_all:
                    # print(d)
                    if (d['creator']['email'] != CREATOR_EMAIL) & (d['start']['dateTime'] > datetime.datetime.utcnow().isoformat()) & (d['start']['dateTime'] < (datetime.datetime.utcnow() + datetime.timedelta(days=deadline_days)).isoformat()):
                        stCal_out.append(d['start']['dateTime'])
                        enCal_out.append(d['end']['dateTime'])
                        summaryCal_out.append(d['summary'])
                        creatorCal_out.append(d['creator']['email'])
                        idCal_out.append(d['id'])
                        # pinCal_out.append(cal['location'])
                        # dbCal_out.append(cal['description'].split(';')[0])
                        # sedeCal_out.append(cal['description'].split(';')[1])
                
                if not len(stCal_out)>0:
                    print(f'Nothing to add to {l}')
                    continue
                
                df_cal_out = pd.DataFrame({'Inizio':stCal_out, 'Fine':enCal_out})
                df_out = pd.merge(df_cal_out, df_t, left_on = ['Inizio', 'Fine'], right_on = ['Inizio', 'Fine'], how="left", indicator=True).query('_merge=="left_only"')
        #         print(df_out)
                l_out_inizio = df_out['Inizio'].tolist()
                l_out_fine = df_out['Fine'].tolist()
                for d in range(len(l_out_inizio)):
                    # logging.info(f'{d+1}: Deleting event {l}, at {dl_inizio[d]}')
                    print(f'{d+1}: Adding event {l} to Innovaplan, at {l_out_inizio[d]}')
        #            print(service.events().list(calendarId=calId, timeMin=dl_inizio[d], timeMax=dl_fine[d]).execute())
                    print(cal['location'])
                    # print(l_out_inizio[d])
                    # print(l_out_fine[d])
        
                    # print(cal['description'].split(';')[0])
                    # print(cal['description'].split(';')[1])
                    # query = f"DECLARE @return_value int \
                    #             EXEC @return_value = [dbo].[spPrenotaAula]\
                    #             @Pin = '12345',\
                    #             @Inizio = {stCal_out[d]},\
                    #             @Fine = {enCal_out[d]},\
                    #             @NoteLocale = {summaryCal_out[d]}\
                    #         SELECT 'Return Value' = @return_value\
                    #         GO"
                    # query = f"DECLARE	@return_value int\
                    #             EXEC	@return_value = [dbo].[spPrenotaAula]\
                    #             @Pin {cal['location']},\
                    #             @Inizio {stCal_out[d]}, \
                    #             @Fine {enCal_out[d]}, \
                    #             @NoteLocale {summaryCal_out[d]},\
                    #             @email {creatorCal_out[d]},\
                    #             @DB {cal['description'].split(';')[0]},\
                    #             @Sede {cal['description'].split(';')[1]}"
                    print(type(stCal_out[d]))
                    st = datetime.datetime.fromisoformat(stCal_out[d]).strftime("%Y-%m-%d %H:%M:%S.%f").replace('.000000','.000')
                    en = datetime.datetime.fromisoformat(enCal_out[d]).strftime("%Y-%m-%d %H:%M:%S.%f").replace('.000000','.000')
                    # en = datetime.datetime.strptime(enCal_out[d], "%Y/%m/%d %H:%M:%S.%f")
                    query = f"{cal['location']},\
                                {st},\
                                {en},\
                                {summaryCal_out[d]},\
                                {creatorCal_out[d]},\
                                {cal['description'].split(';')[0]},\
                                {cal['description'].split(';')[1]}"
                    print("Query: \n", query)
                    cursor = conn.cursor()
                    cursor.execute(query)
                    # commit the transaction
                    cmm = conn.commit()
                    print(cmm)

                    # service.events().insert(calendarId=calId,
                    # body={
                    # "summary": l,
                    # "start": {"dateTime": stCal_out[d], "timeZone": timezone},
                    # "end": {"dateTime": enCal_out[d], "timeZone": timezone},
                    # }
                    # ).execute()

                    # service.events().delete(calendarId =calId, eventId = idCal_out[d]).execute()
                    # # Establishing connection
                   
                    # cursor = conn.cursor()
                    # cursor.execute(query)
                    # # commit the transaction
                    # cmm = conn.commit()
                    # print(cmm)
                
            except Exception as E:
                print(E)
                # print(f'Error!')
            except Exception as e:        
                logging.error(e)
                # if '403' in str(e):
                #     logging.error('403')
                # if c_attempt < deadline:
                #     logging.error(f'Reattempt #{c_attempt+1}')
                #     print(f'Reattempt #{c_attempt+1}')
                #     logging.error(f'Waiting for {waittime}s')
                #     print(f'Waiting for {waittime}s')
                #     sleep((waittime)+np.random.random())
                #     addEventToGoogle(df, service, locali = locali, attempt = c_attempt+1)
                # else:
                #     logging.error(f'Reattempt finished')
                #     print(f'Reattempt finished')
                    
                    
        #         # print(e)
    finally:
	
        return True
    
