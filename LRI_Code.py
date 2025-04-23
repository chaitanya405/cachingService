import mysql.connector
import logging
class Node:
    
    def __init__(self,key,value) :
        self.key = key
        self.value = value
        self.prev = None
        self.next = None

class mysqlDb:
    
    def __init__(self,config) :
        self.host = config["host"]
        self.user = config["user"]
        self.password = config["password"]
    
    def connect(self) :
        try :
            self.conn = mysql.connector.connect(host=self.host,user=self.user,password=self.password)
            self.cursor = self.conn.cursor()
            self.cursor.execute("create database IF NOT EXISTS mysql")
            self.cursor.execute("create table IF NOT EXISTS mysql.mytable ( id varchar(10), state varchar(50));")
            logging.info("Connected to Database")
            return True
        except mysql.connector.Error as err :
            logging.error("Error in connecting to database")
            raise
                
    def getDataFromDb(self,key) : 
        try :
            sql_query = f"select state from mysql.mytable where id = {key}" 
            self.connect()              
            self.cursor.execute(sql_query)
            output=self.cursor.fetchall()
            logging.info("Fetched the Data from database") 
            self.cursor.close()            
            return output[0]       
        except mysql.connector.Error as err :
            logging.error("Error in connecting to database")
            raise  
    def getAllDataFromDb(self) : 
        try :
            sql_query = f"select * from mysql.mytable" 
            self.connect() 
            self.cursor.execute(sql_query)
            output=self.cursor.fetchall()
            logging.info("Fetched the Data from database") 
            self.cursor.close()            
            print(output)
        except mysql.connector.Error as err :
            logging.error("Error in connecting to database")
            raise  

    def delDataFromDb(self,key) :
        try :           
            del_query = f"DELETE from mysql.mytable where id = {key}"
            self.connect() 
            self.cursor.execute(del_query)
            self.conn.commit()
            logging.info("Deleted the Data from database")  
            self.cursor.close() 
            logging.info("Database connection closed")           
        except mysql.connector.Error as err :
            logging.error("Error in deleting from database")
            raise
        
    
    #def dbClose(self,delcursor) :    
    #    delcursor.close()
        #self.db.close()
        #logging.info("Database connection closed")
        

class testCache:
    
    def __init__(self,max_elements) :
        self.max_elements = max_elements
        self.cache = {}
        
        #initializing head and tail nodes
        self.head = Node(None,None)
        self.tail = Node(None,None)
        
        # linking the head and tail       
        self.head.next = self.tail
        self.head.prev = self.head
    
    def removeNode(self, node) :
        prev_node = node.prev
        next_node= node.next
        prev_node.next = next_node
        next_node.prev = prev_node
        
    def add_to_the_top(self, node) :
        node.next = self.head.next
        node.prev = self.head
        self.head.next.prev = node
        self.head.next = node
        
        
    def getCache(self, key,db_config) :
        try :
            if key in self.cache :
               node = self.cache[key]
               self.removeNode(node)
               self.add_to_the_top(node)
               return node.value
            else :
               db = mysqlDb(db_config)            
               result = db.getDataFromDb(key)
               if result :
                   value = result[0]
                   self.putCache(key,value)
                   return value
               else :
                   logging.info("Cannot Retrieve the data from Cache or Database")
                   
        except Exception as e :
               logging.error("Cannot Retrieve the data")
               raise
    
    def putCache(self,key,value) :
        if key in self.cache :
            node = self.cache[key]
            self.removeNode(node)
        else :
            if len(self.cache) >= self.max_elements :
                remove_node = self.tail.prev
                self.removeNode(remove_node)
                del self.cache[remove_node.key]
                logging.info(" The least used key which is at the end has been removed from cache")
            node = Node(key, value)
            self.add_to_the_top(node)
            self.cache[key] = node
                
            
    def removeData(self,key,db_config) :
        try :
            if key in self.cache :
                node = self.cache[key]
                self.removeNode(node)
                del self.cache[key]
                logging.info("Data deleted from Cache")  
                logging.info("Deleting data from database")                
                db = mysqlDb(db_config)                
                db.delDataFromDb(key)
        except Exception as e :
               logging.error("Cannot Delete the data")
               raise        

    def removeAllData(self,db_config) :
        try :
            for key in list(self.cache):
                self.removeData(key,db_config)
        except Exception as e :
               logging.error("Cannot Delete the Cache")
               raise                 
    def clearCache(self,cache_data) :
        cache_data.clear() 
    def getCacheData(self) :
        cache_data = {}
        for key in list(self.cache) :
            node = self.cache[key]
            cache_data[key] = node.value
        print(cache_data)    
        
              
                            

#if __name__ == "main" :
db_config = { "host" : "localhost" , "user" : "myuser", "password" : "password"}
    
    
    # connecting to database 
    
    
    
    #  Initializing Cache Capacity to 3 elements 
    
tempCache = testCache(3)

dbData = mysqlDb(db_config)
# Current data from the database 
dbData.getAllDataFromDb()    
    # Adding data to Cache
tempCache.getCache('1',db_config)
tempCache.getCache('2',db_config)
tempCache.getCache('3',db_config)
tempCache.getCacheData()

# Current data from the database 
dbData.getAllDataFromDb() 
    
    # Getting the elemnt values from cache
print(tempCache.getCache('1',db_config))
print(tempCache.getCache('2',db_config))

print(tempCache.getCache('6',db_config))
    
    #Adding extra element more than capacity 
tempCache.getCache('4' , db_config)
    
    # Checking the cache to see the updated elements after deleting the least used one
tempCache.getCacheData()

# Current data from the database 
dbData.getAllDataFromDb() 
    
    # Removing the data from cache
tempCache.removeData('1',db_config)
    
tempCache.getCacheData()
# Current data from the database 
dbData.getAllDataFromDb() 
    
    # Removing all the data from cache and Database
tempCache.removeAllData(db_config)

tempCache.getCacheData()
# Current data from the database 
dbData.getAllDataFromDb() 
    
    # Adding the element to cache
    
tempCache.getCache('5' , db_config)
tempCache.getCacheData()
    
    # Clearing the Cache
tempCache.clearCache(tempCache.cache)  
    
tempCache.getCacheData()  

# Current data from the database 
dbData.getAllDataFromDb() 

