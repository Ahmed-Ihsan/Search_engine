from concurrent.futures import thread
import sqlite3
import time
from threading import Thread

class search_engine():
    
    def __init__(self,db_file_name):
        self.db_file_name = db_file_name
        self.list_TC = dict()
        self.resl = set()
        con = sqlite3.connect(db_file_name)
        cursor = con.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
        list_table = [ i[0] for i in cursor.fetchall()]
        for j in list_table :
            cursor.execute(f"SELECT * FROM {j};")
            field_names = [i[0] for i in cursor.description]
            self.list_TC[j] = field_names
        con.close()

    def table(self):
        return [i for i in self.list_TC ]

    def col(self,table = None):
        if table != None:
            return self.list_TC[table]
        return self.list_TC
    
    def CTF(self , *tabel):
        con = sqlite3.connect('search_engine.db')
        
        check = sengine.search_table()
        if len(check) > 0 :
            return 1

        con.execute('''CREATE TABLE TABEL
         (ID INT PRIMARY KEY     NOT NULL,
         NAME           TEXT    NOT NULL);''')

        for cont ,i in enumerate(self.table()):
            con.execute(f"INSERT INTO TABEL (ID,NAME) VALUES (?,?)",(cont,i));
        con.commit()

        con.execute('''CREATE TABLE COLUMN
        (ID INT PRIMARY KEY     NOT NULL,
        TAPLE           TEXT    NOT NULL,
        NAME            TEXT    NOT NULL);''')

        index = 0 
        for i in self.table():
            for j in self.col(i):
                index += 1
                con.execute(f"INSERT INTO COLUMN (ID,TAPLE,NAME) \
                                VALUES (?,?,?)",(index, i , j));
            con.commit()
        con.close()
    
    def search_table(self):
        con = sqlite3.connect('search_engine.db')
        cur = con.cursor()
        cur.execute("SELECT name FROM sqlite_master WHERE type='table';")
        list_table = [ i[0] for i in cur.fetchall()]
        list_res = []
        for i in list_table:
            resl = cur.execute(f"SELECT * from {i}")
            list_res.append({i:resl.fetchall()})
        con.close()
        return list_res

    def conter_value(self):
        con = sqlite3.connect(self.db_file_name)
        con2 = sqlite3.connect('search_engine.db')
        cur = con.cursor()
        list_table = sengine.table()
        try:
            for i in self.table():
                con2.execute(f'''CREATE TABLE VOLUE
                (ID INT PRIMARY KEY     NOT NULL,
                COL           TEXT    NOT NULL,
                NAME            TEXT    NOT NULL);''')

            list_data = set()
            for i in list_table:
                for j in sengine.col(i):
                    close = ["id",'password']
                    if not j in close:
                        resl = cur.execute(f"SELECT DISTINCT {j} from {i}")
                        for l in resl:
                            list_data.add((l[0] , j))
            
            for cont , i in enumerate(list_data):
                con2.execute(f"INSERT INTO VOLUE (ID,COL,NAME) \
                                    VALUES (?,?,?)",(cont, i[1] , i[0]));
            con2.commit()
        except Exception as e:
            # print(e)
            pass
        
        con2.close()
        con.close()

        return 0

    def view_val(self, list_data = 'None' ):
        con = sqlite3.connect('search_engine.db')
        cur = con.cursor()
        list_resl = []
        resl_select = list()
        for i in list_data:
            if list_data != 'None':
                list_resl.append(cur.execute(f"SELECT * from VOLUE WHERE COL='{i}'"))
                resl_select.append([j for i in list_resl for j in i])
            else:
                list_resl.append(cur.execute(f"SELECT * from VOLUE"))
                resl_select.append([j for i in list_resl for j in i])
                break
        return resl_select
    
    def search(self,key_words , *list_data):
        self.resl = set()
        if len(list_data) == 0:
            list_data = self.view_val()
        else:
            list_data = self.view_val(list_data)
        if type(key_words) == str:
            key_words = key_words.split()
            dict_thread = dict()
            for cont , word in enumerate(key_words):
                dict_thread[f'{cont}'] = Thread(target=save_reslt,args=(list_data,word,self.resl))
            for i in dict_thread:
                dict_thread[i].start()
            for i in dict_thread:
                try:
                    dict_thread[i].join()
                except:
                    pass
        return self.resl

def save_reslt(list_data,word,reslt):
    for j in list_data:
        for i in j:
            if i[2] == word:
                reslt.add((i[2],i[1]))
                

sengine = search_engine('db.db')
sengine.CTF()
sengine.conter_value()

# for i in sengine.view_val():
#     print(i)
# for _ in range(10):
#     start = time.time()
#     sengine.view_val()
#     print(time.time() - start)
# print(sengine.table())
# print(sengine.col())
# print(sengine.col('files'))
# print(sengine.search_table())
# for i in sengine.search_table():
#     print(i)
def test(text):
    start = time.time()
    resl = sengine.search(text)
    end = time.time() - start
    word = list()
    col = list()
    for i in resl.union():
        print(f'found {i[0]} in : {i[1]}')
        if not i[0] in word:
            word.append(i[0])
            col.append(i[1])
    for i in range(len(word)):
        print(f'you can search in for <{word[i]}> in <{col[i]}>')
    print(f'found reslt : {len(resl)}')
    print(end)


test('test ahmed 12 ali admin test')
test('ahmed')
test('123')
test('1 2 3 4 5 6 7 8 98 10 11 23  56 8563 ahmed test')

thread_start = dict()
for i in range(200):
    thread_start[f'{i}'] = Thread(target=test,args=(['ahmed']))

for i in thread_start:
    thread_start[i].start()

# for i in thread_start:
#     try:
#         thread_start[i].join()
#     except:
        # pass