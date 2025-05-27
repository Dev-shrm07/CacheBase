import time
import threading

class CACHE_STORE:
    def __init__(self):
        self.lock = threading.Lock()
        self.data = {}
        self.expiry = {}
        
    def get(self, key):
        with self.lock:
            if key in self.expiry.keys():
                if time.time() > self.expiry[key]:
                    del self.expiry[key]
                    del self.data[key]
                    return None
            
            return self.data.get(key,None)
        
    
    def set(self,key,val,ex=None):
        with self.lock:
            self.data[key] = val
            if ex and isinstance(ex,int):
                self.expiry[key] = time.time() + ex
            
            return "OK"
        
    
    def delete(self,key):
        with self.lock:
            if key in self.expiry.keys():
                if time.time() > self.expiry[key]:
                    del self.expiry[key]
                    del self.data[key]
                    return 1
            
            return "OK" if key in self.data.keys() else None
        
    
    def clean_up_expiry(self):
        with self.lock:
            for key in self.expiry.keys():
                if time.time()>self.expiry[key]:
                    del self.expiry[key]
                    del self.data[key]
        
                
        
        
        
        