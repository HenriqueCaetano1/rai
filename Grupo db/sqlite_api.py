#!/usr/bin/env python
# coding: utf-8

# In[1]:


import sqlite3
from sqlite3 import Error
import datetime

# In[2]:


try:
    sqliteConnection = sqlite3.connect('SQLite_Python.db')
    cursor = sqliteConnection.cursor()
    print("Database created and Successfully Connected to SQLite")

    sqlite_select_Query = "select sqlite_version();"
    cursor.execute(sqlite_select_Query)
    record = cursor.fetchall()
    print("SQLite Database Version is: ", record)
    cursor.close()

except sqlite3.Error as error:
    print("Error while connecting to sqlite", error)
finally:
    if (sqliteConnection):
        sqliteConnection.close()
        print("The SQLite connection is closed")


# In[3]:


# Passar como argumento o nome e um dicionário com as variáveis.
# Dict: Keys = nome das variáveis a serem registradas. Values = tipo dos dados que serão armazenadas na coluna.
def createTable(name,var_dict):   
    # Conferir se o argumento é um dicionário.
    if type(var_dict) != dict:
        print("Por favor, passe um dicionário com as variáveis como argumento.")
        return
    # Conferir se o dicionário está vazio. 
    if not(var_dict):
        print("Nenhuma variável foi passada como argumento. Por favor, repita o processo.")
        return
    
    # Colocar as variáveis dadas no formato sql.
    query = '(ID INTEGER PRIMARY KEY AUTOINCREMENT'
    for atributos in var_dict:
        query = query +  ', '
        if var_dict[atributos] == int:
            query = query + str(atributos) + ' ' + 'int'
        if var_dict[atributos] == str or str(var_dict[atributos]) == 'string' or str(var_dict[atributos]) == 'String':
            query = query + str(atributos) + ' ' + 'text'      
    query = query + ' )'
    
    
    # Tentar criar o banco de dados
    try:
        sqliteConnection = sqlite3.connect(DB_FILE)
        sql_query = 'CREATE TABLE IF NOT EXISTS '+name+' '+query
        cursor = sqliteConnection.cursor()
        print("Successfully Connected to SQLite")
        cursor.execute(sql_query)
        sqliteConnection.commit()
        print("SQLite table created")
        cursor.close()
    except sqlite3.Error as error:
        print("Error while creating a sqlite table", error)
    finally:
        if (sqliteConnection):
            sqliteConnection.close()
            print("sqlite connection is closed")
    


# In[4]:


#deleta completamente uma table do db
#deve ser passado o nome da table, como string

def dropTable(name):
    try:
        sqliteConnection = sqlite3.connect(DB_FILE)
        sql_query = '''DROP  TABLE '''+name
        cursor = sqliteConnection.cursor()
        print("Successfully Connected to SQLite")
        cursor.execute(sql_query)
        sqliteConnection.commit()
        print("SQLite table deleted")
        cursor.close()
    except sqlite3.Error as error:
        print("Error while creating a sqlite table", error)
    finally:
        if (sqliteConnection):
            sqliteConnection.close()
            print("sqlite connection is closed")


# In[5]:


#cria uma nova linha em uma determinada tabela
#table -> string, indica em qual tabela os dados serão adicionados 
#columns -> tupla, onde cada elemento indica o nome da coluna em que o dado será adicionado
#values -> tupla, onde cada elemento indica o que será adicionado na coluna descrita na variável columns (em ordem)
def createSqliteRow(table, columns, values):
    try:
        sqliteConnection = sqlite3.connect(DB_FILE)
        cursor = sqliteConnection.cursor()
        print("Successfully Connected to SQLite")
        sqlite_insert_query = 'INSERT INTO '+table+' '+str(columns)+' VALUES '+str(values)
        print(sqlite_insert_query)
        count = cursor.execute(sqlite_insert_query)
        sqliteConnection.commit()
        print("Record inserted successfully into "+table)
        cursor.close()

    except sqlite3.Error as error:
        print("Failed to insert data into sqlite table", error)
    finally:
        if (sqliteConnection):
            sqliteConnection.close()
            print("The SQLite connection is closed")


# In[6]:


#atualiza uma determinada linha em uma determinada tabela
#table -> nome da tabela, string
#row -> id da linha a ser atualizada, int
#columns -> colunas que serão atualizadas naquela linha, tupla (coluna1, coluna2, coluna3, ...)
#values -> valores a ser colocados em cada uma das colunas previamente definidas, tupla (valor1, valor2, valor3, ...)

def updateSqliteRow(table,row,columns,values):
    try:
        sqliteConnection = sqlite3.connect(DB_FILE)
        cursor = sqliteConnection.cursor()
        print("Successfully Connected to SQLite")
        sqlite_insert_query = 'UPDATE '+table+' set '
        for col in columns:
            sqlite_insert_query+=col+' = ?, '
        sqlite_insert_query = sqlite_insert_query[:len(sqlite_insert_query) - 2]
        sqlite_insert_query+=' WHERE ID = '+str(row)
        count = cursor.execute(sqlite_insert_query,values)
        sqliteConnection.commit()
        print("Successfully updated row "+str(row)+" from table "+table)
        cursor.close()

    except sqlite3.Error as error:
        print("Failed to insert data into sqlite table", error)
    finally:
        if (sqliteConnection):
            sqliteConnection.close()
            print("The SQLite connection is closed")


# In[7]:


#seleciona um (ou mais) valores de uma linha
#table, string, tabela a ser retirado o valor
#id, int, id da linha da qual os valores serão selecionados
#columns, tupla, quais colunas terão seus valores selecionados (coluna1, coluna2, ...)

def selectValuesFromId(table,id, columns):
    try:
        sqliteConnection = sqlite3.connect(DB_FILE)
        cursor = sqliteConnection.cursor()
        print("Connected to SQLite")

        sql_select_query = 'select '
        for c in columns:
            sql_select_query+=str(c)+', '
        sql_select_query = sql_select_query[:-2]+' from '+table+' where id = '+str(id)
        print(sql_select_query)
        cursor.execute(sql_select_query)
        records = cursor.fetchall()
        print("Successfully selected row "+str(id)+" from table "+table)
        cursor.close()
    except sqlite3.Error as error:
        print("Failed to read data from sqlite table", error)
        return 0
    finally:
        if (sqliteConnection):
            sqliteConnection.close()
            print("The SQLite connection is closed")
        return records
            


# In[8]:


#deleta uma linha na tabela, baseada no id da mesma
#table, string, tabela da qual a linha sera deletada
#id, int, id da linha a ser deletada

def deleteRowFromId(table,id):
    try:
        sqliteConnection = sqlite3.connect(DB_FILE)
        cursor = sqliteConnection.cursor()
        print("Connected to SQLite")

        sql_select_query = 'DELETE from '+table+' where id = ?'
        print(sql_select_query)
        cursor.execute(sql_select_query,(id, ))
        sqliteConnection.commit()
        records = cursor.fetchall()
        print("Successfully deleted row "+str(id)+" from table "+table)
        cursor.close()
    except sqlite3.Error as error:
        print("Failed to read data from sqlite table", error)
    finally:
        if (sqliteConnection):
            sqliteConnection.close()
            print("The SQLite connection is closed")
        


# In[9]:


#retorna o ultimo valor adicionado em uma determinada tabela
#table, string, tabela de onde o valor será retirado

def getLastRow(table):
    try:
        sqliteConnection = sqlite3.connect(DB_FILE)
        cursor = sqliteConnection.cursor()
        print("Connected to SQLite")

        sqlite_select_query = 'SELECT * FROM '+ table +' ORDER BY ID DESC LIMIT 1'
        cursor.execute(sqlite_select_query)
        sqliteConnection.commit()
        records = cursor.fetchall()
        print("Successfully selected row last row from table "+table)
        cursor.close()
    except sqlite3.Error as error:
        print("Failed to read data from sqlite table", error)
    finally:
        if (sqliteConnection):
            sqliteConnection.close()
            print("The SQLite connection is closed")
            return records


# In[10]:


def deleteLastRow(table):
    try:
        sqliteConnection = sqlite3.connect(DB_FILE)
        cursor = sqliteConnection.cursor()
        print("Connected to SQLite")

        sqlite_select_query = 'DELETE FROM '+ table +' WHERE ID = '+'(SELECT MAX(id) FROM '+table+')'
        cursor.execute(sqlite_select_query)
        sqliteConnection.commit()
        records = cursor.fetchall()
        print("Successfully deleted last row from table "+table)
        cursor.close()
    except sqlite3.Error as error:
        print("Failed to read data from sqlite table", error)
    finally:
        if (sqliteConnection):
            sqliteConnection.close()
            print("The SQLite connection is closed")


# In[11]:


#leia a tabela em questão
#cada linha da tabela sera retornada como uma tupla
#table, string, nome da tabela a ser mostrada

def readSqliteTable(table):
    try:
        sqliteConnection = sqlite3.connect(DB_FILE)
        cursor = sqliteConnection.cursor()
        print("Connected to SQLite")

        sqlite_select_query = 'SELECT * FROM '+table
        cursor.execute(sqlite_select_query)
        records = cursor.fetchall()
        print("Total rows are:  ", len(records))
        print("Printing each row")
        for row in records:
            print(row)
        cursor.close()

    except sqlite3.Error as error:
        print("Failed to read data from sqlite table", error)
    finally:
        if (sqliteConnection):
            sqliteConnection.close()
            print("The SQLite connection is closed")


# In[12]:


DB_FILE = 'SQLite_Python.db'


# In[13]:


dropTable('sensores')


# In[14]:


dict1 = {'teste1': int, 'teste2': 'string'}
createTable('sensores',dict1)


# In[15]:


createSqliteRow('sensores',('teste1','teste2'),(9,'tchau'))


# In[16]:


updateSqliteRow('sensores',2,('teste1','teste2'),(3,'oi'))


# In[17]:


readSqliteTable('sensores')


# In[18]:


getLastRow('sensores')


# In[19]:


getLastRow('sensores')


# In[20]:


selectValuesFromId('sensores',2,('teste1','teste2'))

class Users_Info:
    
    def __init__(self):
        global db_file 
        db_file = "Users_Information.db"

    def __Create_connection(self):
        """ create a database connection to the SQLite database
            specified by db_file \n
        :param db_file: database file \n
        :return: Connection object or None
        """
        
        conn = None
        try:
            conn = sqlite3.connect(db_file)
            return conn
        except Error as e:
            print(e.message)    
        return conn
    
    def __Create_table(self, db_name, conn):
        """ create a table from the create_table_sql statement
        :param conn: Connection object \n
        :return:
        """
        query = self.__Getquery(db_name)
        
        try:
            c = conn.cursor()
            c.execute(query)
            conn.commit()
            print("SQLite table created")
        except Error as e:
            print(e)
            
    def __Getquery(self, db_tablename):
        sql_create_UsersInfo_table = """CREATE TABLE IF NOT EXISTS """+db_tablename+""" (
                                        id integer PRIMARY KEY AUTOINCREMENT,
                                        username text NOT NULL,
                                        methodcalling text,
                                        query text,
                                        created_at timestamp); """
        return sql_create_UsersInfo_table
    
    def Add_UserActivity(self, username, method_calling, query):
        """Adiciona atividade do usuário. 
            :param username: Nome do usuário em string \n
            :param method_calling: Utilizado no script de python, passa a função que foi chamada \n
            :param query: Contém a informação da operação realizada
        """
        query_sql = """INSERT INTO {}
                          (username, methodcalling, query, created_at) 
                          VALUES (?, ?, ?, ?);""".format(username)
                          
        datatuple = (username, method_calling, query, datetime.datetime.now())
        
        try:
           conn = self.__Create_connection() 
           self.__Create_table(username, conn)
           cursor = conn.cursor()
           cursor.execute(query_sql, datatuple)
           conn.commit()
           conn.close()
           print("Informações registradas  com sucesso! \n")
        except Error as e:
            print("O seguinte erro aconteceu:{}".format(e))

