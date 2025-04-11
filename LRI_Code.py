import mysql.connector
import logging
class Node :
    
    def __init__(self,key,value) :
        self.key = key
        self.value = value
        self.prev = None
        self.next = None

class mysqlDb :
    
    def __init__(self,host,user,password,database) :
        self.host = host
        self.user = user
        self.password = password
        self.database = database
    
    def connect(self,config) :
        try :
            self.conn = mysql.connector.connect(host=self.host,user=self.user,password=self.password,database=self.database)
            self.cursor = self.conn.cursor()
            logging.info("Connected to Database")
            return True
        except mysql.connector.Error as err :
            logging.error("Error in connecting to database")
            raise
                
    def getDataFromDb(self,key,config) : 
        try :
            sql_query = f"select* from database.table where id = {key}"                
            self.cursor.execute(sql)
            output=self.cursor.fetchall()
            logging.info("Fetched the Data from database") 
            self.dbClose()            
            return output       
        except mysql.connector.Error as err :
            logging.error("Error in connecting to database")
            raise  
    
    def delDataFromDb(self,key,config) :
        try :           
            del_query = f"DELETE from database.table where id = {key}"
            self.cursor.execute(del_sql)
            self.conn.commit()
            logging.info("Deleted the Data from database")  
            self.dbClose()            
        except mysql.connector.Error as err :
            logging.error("Error in deleting from database")
            raise
        
    
    def dbClose() :    
        self.mycusror.close()
        self.db.close()
        logging.info("Database connection closed")
        

class testCache :
    
    def _init_(self,max_elements) :
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
                   db.dbClose()
                   return value
               else :
                   logging.info("Cannot Retrieve the data from Ccahe or Database")
                   
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
                db.dbClose()
        except Exception as e :
               logging.error("Cannot Delete the data")
               raise        

    def removeAllData(self,cache_data,db_config) :
        try :
            for key in cache_data:
                self.removeData(key,db_config)
        except Exception as e :
               logging.error("Cannot Delete the Cache")
               raise                 
    def clearCache(self,cache_data) :
        cache_data.clear() 
    def getCacheData(self) :
        return self.cache        
         
                   

if __name__ == "main" :
    db_config = { "host" : "host" , "user" : "user", "password" : "password", "database" : "database"}
    
    
    # connecting to database 
    
    
    
    #  Initializing Cache Capacity to 3 elements 
    
    tempCache = testCache(3)
    
    # Adding data to Cache
    tempCache.putCache('1' , 'New Jersey')
    tempCache.putCache('2' , 'Delaware')
    tempCache.putCache('3' , 'New York')
    
    # Getting the elemnt values from cache
    print(tempCache.getCache('1',db_config))
    print(tempCache.getCache('2',db_config))
    
    #Adding extra element more than capacity 
    tempCache.putCache('4' , 'California')
    
    # Checking the cache to see the updated elements after deleting the least used one
    
    print(tempCache.getCacheData())
    
    # Removing the data from cache
    tempCache.removeData('4',db_config)
    
    print(tempCache.getCacheData())
    
    # Removing all the data from cache and Database
    tempCache.removeAllData(cache,db_config)
    print(tempCache.getCacheData())
    
    # Adding the element to cache
    
    tempCache.putCache('5' , 'Florida')
    
    # Clearing the Cache
    tempCache.clearCache(cache)  
    
    print(tempCache.getCacheData())    
    
    
    
    
    
    
    
    
