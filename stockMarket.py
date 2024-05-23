from app.NseHandler import NSEHandler
# from DataHandlers import dateFormat #generate_time_intervals, get_unique, dlist_dict, setNum, valreplace
import time
star = time.time()

nse = NSEHandler(False)
fom:str ='%Y-%m-%d'
# folder = './app/data'

# prefor = '%d-%b-%Y'
# print(prefor)

# nse.dbConn.execute("CREATE INDEX FnO_Bhavcopy_idx ON FnO_Bhavcopy (date,FinInstrmNm,TckrSymb,XpryDt,StrkPric);") 
# print(nse._get_fno_bhavdata('2024-05-02', 'OPTIDX', 'NIFTY', 'CE', '2024-05-09', format=fom)[0])

# data = nse.dbConn.getTbData('FnO_Bhavcopy')
# print(type(data))

nse.dbConn.get_excel('FnO_Bhavcopy')

en = time.time()
execu = en - star
print(f'Time taken to run {execu} seconds')



# , 'RptgDt':dateFormat(i['RptgDt'], prefor, fom)

# dbConn.export_data() # creates the backup of the database         
        
