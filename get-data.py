
#File to run all data
# Import system necesary library
import threading, datetime , sys, json, os, argparse, logging, time, shutil


#conexion a la base de datos
from datab import DatabaseOperation
from pagedata import PageApp
from postdata import PostApp


#Database conection
DB = DatabaseOperation()

# TODO: use the latest version available
VERSION = '3.1'

def remove_folder(path):
    if os.path.exists(path):
        shutil.rmtree(path)
    else:
        print('No existe temp')

def run_page_thread(**page_data):
    page_data = page_data['page_data']
    page_id = page_data.id_page
    now = datetime.datetime.now()
    deltaCurrent = now.strftime('%Y-%m-%d %X')
    deltalog = now.strftime('%Y-%m-%d-%X')
    print(' Start to run Page ', page_id, now.year, now.month, now.day, now.hour, now.minute, now.second)
    #Update Status
    updated= {
        'date_run_start': deltaCurrent,
        'status':'R'
    }
    DB.update('token', 'id_page', page_id, updated)

    #hilo1 = threading.Thread(target=PageApp,args=(VERSION, page_data))
    hilo2 = threading.Thread(target=PostApp,args=(VERSION, page_data))
    #hilo1.start()
    hilo2.start()
    # Del Date Variables
    del now
    del deltaCurrent
    del deltalog
    
def main():
    print('Running main')
    if not os.path.exists('temp'):
        os.makedirs('temp')

    page_data = DB.select('token')

    for page in page_data :
        hilo = threading.Thread(target=run_page_thread,
                                kwargs={
                                    'page_data':page
                                })
        hilo.start()

if __name__ == '__main__':
    main()