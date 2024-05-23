from FileHandler import read
from SqliteHandler import SqliteHandler
from DataHandlers import dlist_dict, list_dlist
from datetime import datetime


DBCONN:dict[str, list] = {}

def setup_db(database:str='', pathway='./app/database')->None:
    """
        setup_db()
        ----------
        This method is responsible for making any necessary tables in the db if not present.
    """
    global DBCONN
    print('Running Db Setup....')
    tables:list
    table_info = read('./app/data/db_setup.json', decode_json=True)
    if database=='':
        for k,v in table_info.items():
            connect_db(k, pathway)
            tables = DBCONN[k].getTb()
            for i in v:
                if i['table_name'] not in tables: 
                    if DBCONN[k].createTb(i['table_name'], i['column_names'], addUnique=i['unique'], indexCol=i['indexes']):            
                       print(f"Table('{i['table_name']}') created in database('{k}')")
                    else: print(f"Table('{i['table_name']}') creation failed in database('{k}')")
    else:
        connect_db(database, pathway)
        tables = DBCONN[database].getTb()
        for i in table_info[database]:
            if i['table_name'] not in tables: 
                if DBCONN[database].createTb(i['table_name'], i['column_names'], addUnique=i['unique'], indexCol=i['indexes']):
                    print(f"Table('{i['table_name']}') created in database('{database}')")
                else: print(f"Table('{i['table_name']}') creation failed in database('{k}')")

def connect_db(database:str, pathway='.')->object:
    """
        connect_db()
        ------------
        This method connects to the database.
        
        Parameter:
        - `database` str: The name of the database. Example: `Test.db`
        - `pathway` str: The location path, where the database is to be located, example: `./project_folder/database`. Default value is `'.'`, will make the db in the same location. 
    
    """
    global DBCONN
    if database not in DBCONN.keys():
        DBCONN[database] = SqliteHandler(database, pathway)
    return DBCONN[database]

def local_runner(self=None, data:dict={})->None:
    """
        local_runner()
        --------------
        This method runs some funcions, that are neccessary for making the programm run properly.
        
        Parameters:
        - data dict: Takes the key and dict where the info is located of the function to be run.
            Usage Example:
            ```
                func_data = {
                'func_name_str': {"func": func_source_here, "params":[{..}, {..},...], "runtime": {"before": date_in_str, "after":date_in_str}},
                .....
                }
            ```
    """
    val:list= []
    ret:int = 0
    current_time = datetime.now()
    if data!={}:
        for k,v in data.items():
            print(f'{k} Automaticaly running functions', end='....')
            try:
                func = getattr(self, v['func'])
                if isinstance(v['params'], list):
                    for vl in v['params']:
                        func(**vl)
                else:
                    func(**v['params'])
                val.append(k)
                print(f'func: {str(k)} Runned..')
            except:
                print(f'func: {str(k)} failed..')
    else:
        for k,v in read('./app/data/daily_runner.json', decode_json=True).items():
            
            local_runner()

def _handle_csv(self, data:str)->dict:
    """
        _handle_csv
        -----------
        This method converts the csv data (cointaing headers) into a dict.
    """
    data = [i.split(',') for i in data.splitlines(False)]
    return dlist_dict(list_dlist(data[1:], data[0]))


