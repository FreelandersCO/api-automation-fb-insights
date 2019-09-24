# Import system necesary library
import sys, json, os, argparse, logging, time, requests, calendar, facebook, threading
from datetime import datetime
#import pandas as pd
from datab import DatabaseOperation
VERSION = "3.1"

def print_json(json_print):
    print(json.dumps(json_print, sort_keys=True, indent=4))
    sys.exit(0)

class GetOutOfLoop( Exception ):
    pass

class ConversationApp(object):
    def __init__(self):
        super(ConversationApp).__init__()
        #Log
        now = datetime.now()
        print(' Start to run Conversation', now.year, now.month, now.day, now.hour, now.minute, now.second)

        self.database = DatabaseOperation()
        page_data = self.database.select('token')

        for page in page_data :
            hilo = threading.Thread(target=self.run_conversations_thread,
                                kwargs={
                                    'page_data': page
                                })
            hilo.start()
        
        now = datetime.now()
        print(' End to run Post', now.year, now.month, now.day, now.hour, now.minute, now.second)

    def run_conversations_thread(self, page_data):
        page_id = page_data.id_page
        page_token = page_data.token
        graph = facebook.GraphAPI(access_token=page_token, version=VERSION)
        args = {'fields' : 'id,message_count,updated_time,link,messages.limit(100){id,from,message,sticker,created_time}','limit':499}  #requested fields
        conv = graph.get_object(page_id+'/conversations', **args)
        # print(conv)
        # Wrap this block in a while loop so we can keep paginating requests until
        # finished.
        while True:
            try:
                # Perform some action on each post in the collection we receive from
                # Facebook.
                #[self.conversation_process(mess=convv) for convv in conv['data']]
                for mess in conv['data']:
                    conversation_id = mess['id']
                    message_count = mess['message_count']
                    conversation_db = self.database.select('conversation', 'conversation_id', conversation_id)
                    # print(conversation_db)
                    lon = len(conversation_db)
                    if(lon >0):
                        #Existe (No tiene que traer mas)
                        print('Existe')
                        raise GetOutOfLoop
                    else:
                        #No Existe
                        data_to_database = {}
                        data_to_database['conversation_id'] = conversation_id
                        data_to_database['id_page'] = page_id
                        data_to_database['link'] = mess['link']
                        data_to_database['message_count'] = message_count
                        data_to_database['updated_time'] = mess['updated_time']
                        self.database.insert('conversation',data_to_database)
                        del data_to_database
                        self.message_process(conversation_id,mess['messages']['data'])
                # Attempt to make a request to the next page of data, if it exists.
                time.sleep(1)
                conv = requests.get(conv["paging"]["next"]).json()
            except KeyError:
                # When there are no more pages (['paging']['next']), break from the
                # loop and end the script.
                break
            except GetOutOfLoop:
                break

    def message_process(self, conversation_id, message_data):
        print(conversation_id)
        for message in message_data :
            data_to_database = {}
            data_to_database['message_id'] = message['id']
            data_to_database['conversation_id'] = conversation_id
            data_to_database['from_id'] = message['from']['id']
            data_to_database['from_name'] = message['from']['name']
            data_to_database['from_email'] = message['from']['email']
            try:
                msn=message['message']
            except:
                msn=''
            try:
                strk=message['sticker']
            except:
                strk=''
            data_to_database['message'] = msn
            data_to_database['sticker'] = strk
            data_to_database['message_created_time'] = message['created_time']
            self.database.insert('message',data_to_database)
            del data_to_database
            

def main():
    # TODO: use the latest version available
    ConversationApp()


if __name__ == '__main__':
    main()