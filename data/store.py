import time
import threading
from data.stream import STREAM
from typing import Dict

class CACHE_STORE:
    def __init__(self,expiry):
        self.default_expiry = expiry
        self.lock = threading.Lock()
        self.data: Dict[str:any] = {}
        self.expiry: Dict[str:float] = {}
        self.types: Dict[str:str] = {}
        
        
        
    def get(self, key):
        with self.lock:
            if self.expire_check(key):
                return "ERROR: Key has Expired"
            
            return self.data.get(key,None)
    
        
    
    def set(self,key,val,ex=None):
        with self.lock:
            ex = self.default_expiry
            self.data[key] = val
            self.types[key] = "string"
            if ex and isinstance(ex,int):
                self.expiry[key] = time.time() + ex
            
            return "OK"
        
    
    def delete(self,keys):
        with self.lock:
            flag = 1
            for key in keys:
                if key in self.data.keys():
                    del self.data[key]
                    del self.types[key]
                    if key in self.expiry.keys():
                        del self.expiry[keys]
                
                else:
                    flag = 0
            
            
            return flag
                
                
    def expire(self,key, times):
        with self.lock:
            if key in self.data.keys():
                self.expiry[key] = time.time() + times
                return 1
            return 0
                
                        
    
    def clean_up_expired_keys(self):
        with self.lock:
            for key in self.expiry.keys():
                self.expire_check(key)

                    
            return None
        
    def expire_check(self,key):
        if key in self.expiry.keys() and  time.time()>=self.expiry[key]:
            del self.expiry[key]
            del self.types[key]
            del self.data[key]
            
            return True

        return False
        
        
        
    def xadd(self,key,Fields,id='*',maxLen=None,Approx=False):
        with self.lock:

            if key in self.data.keys() and self.expire_check(key):
                return None
            
            stream = self.data.get(key, STREAM())
            if not isinstance(stream, STREAM):
                stream = STREAM()
            response = stream.xadd(Fields,id,maxLen,Approx)
            self.data[key] = stream
            self.types[key] = "stream"
            self.expiry[key] = time.time() + self.default_expiry
            return response
            
    
    def xdel(self,key,ids):
        with self.lock:
            if key in self.data.keys() and self.expire_check(key):
                return None
            
            if key in self.data.keys():
                if not isinstance(self.data[key], STREAM):
                    return 0
                return self.data[key].xdel(ids)
            
            return 0
        
    
    def xrange(self,key, start='-',end='+', count=None):
        with self.lock:
            if key in self.data.keys() and self.expire_check(key):
                return None
            
            if key in self.data.keys():
                if not isinstance(self.data[key], STREAM):
                    return None
                response =  self.data[key].xrange(start=start,end=end,count=count)
                return response
            
            return None
        
    
    def get_stream_last_id(self,key):
        with self.lock:
            if key in self.data.keys() and self.expire_check(key):
                return '$'
            
            if key in self.data.keys():
                if not isinstance(self.data[key], STREAM):
                    return '$'
                return self.data[key].get_last_id()
            
            return '$'
        
    def xread(self,keys,ids,count=None):
        with self.lock:
            if len(keys)!=len(ids):
                return None
            
            response = []
            for i in range(len(keys)):
                key = keys[i]
                if key not in self.data.keys():
                    D = None
                elif key in self.data.keys() and self.expire_check(key):
                    D = None
                elif key in self.data.keys():   
                    if not isinstance(self.data[key], STREAM):
                        D = None
                else:
                    id = ids[i]
                    D = self.data[key].xread(start=id,count=count)
                response.append([key,D])


            return response
    
    
    def xlen(self,key):
        with self.lock:
            if key in self.data.keys() and self.expire_check(key):
                return None
            
            if key in self.data.keys():
                if not isinstance(self.data[key], STREAM):
                        return None
                return len(self.data[key].data)
            
            return 0
        
        
    def get_type(self,key):
        with self.lock:
            return self.types.get(key,None)
        
            