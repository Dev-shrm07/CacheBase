import time
import threading
from stream import STREAM
from typing import Dict

class CACHE_STORE:
    def __init__(self):
        self.lock = threading.Lock()
        self.data: Dict[str:any] = {}
        self.expiry: Dict[str:float] = {}
        self.streams:Dict[str:STREAM] = {}
        self.types: Dict[str:str] = {}
        
        
        
    def get(self, key):
        with self.lock:
            if self.expire_check(key):
                return "ERROR: Key has Expired"
            
            return self.data.get(key,None)
    
        
    
    def set(self,key,val,ex=None):
        with self.lock:
            if key in self.streams.keys():
                return "ERROR: This Key exists as a STREAM"
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
                    
                elif key in self.streams.keys():
                    del self.streams[key]
                    del self.types[key]
                    if key in self.expiry.keys():
                        del self.expiry[keys]
                
                else:
                    flag = 0
            
            return flag
                
                
    def expire(self,key, times):
        with self.lock:
            if key in self.streams.keys() or key in self.data.keys():
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
            if key in self.data.keys():
                del self.data[key]
                        
            elif key in self.streams.keys():
                del self.streams[key]
            
            return True

        return False
        
        
        
    def xadd(self,key,Fields,id='*',maxLen=None,Approx=False):
        with self.lock:
            if key in self.data.keys():
                return None
            if key in self.streams.keys() and self.expire_check(key):
                return None
            
            if key not in self.streams.keys():
                self.types[key] = "stream"
                
            stream = self.streams.get(key, STREAM())
            response = stream.xadd(Fields,id,maxLen,Approx)
            self.streams[key] = stream
            return response
            
    
    def xdel(self,key,ids):
        with self.lock:
            if key in self.streams.keys() and self.expire_check(key):
                return None
            
            if key in self.streams.keys():
                return self.streams[key].xdel(ids)
            
            return 0
        
    
    def xrange(self,key, start='-',end='+', count=None):
        with self.lock:
            if key in self.streams.keys() and self.expire_check(key):
                return None
            
            if key in self.streams.keys():
                response =  self.streams[key].xrange(start=start,end=end,count=count)
                return response
            
            return None
        
    
    def get_stream_last_id(self,key):
        with self.lock:
            if key in self.streams.keys() and self.expire_check(key):
                return '$'
            
            if key in self.streams.keys():
                return self.streams[key].get_last_id()
            
            return '$'
        
    def xread(self,keys,ids,count=None):
        with self.lock:
            if len(keys)!=len(ids):
                return None
            
            response = []
            for i in range(len(keys)):
                key = keys[i]
                if key not in self.streams.keys():
                    D = None
                elif key in self.streams.keys() and self.expire_check(key):
                    D = None
                else:
                    id = ids[i]
                    D = self.streams[key].xread(start=id,count=count)
                response.append([key,D])


            return response
    
    
    def xlen(self,key):
        with self.lock:
            if key in self.streams.keys() and self.expire_check(key):
                return None
            
            if key in self.streams.keys():
                return len(self.streams[key].data)
            
            return 0
        
        
    def get_type(self,key):
        with self.lock:
            return self.types.get(key,None)
        
            