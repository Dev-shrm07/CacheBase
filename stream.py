
import time
import re
from sortedcontainers import SortedList
from typing import List, Dict



class STREAM:
    def __init__(self):
        self.data: Dict[str:Dict[str:any]] = {}
        self.data_idx = SortedList([])
        self.last_timestamp = None
    
    @staticmethod
    def compare_ids(id1:str, id2:str):
        ts1, c1 = map(int, id1.split('-'))
        ts2, c2 = map(int, id2.split('-'))
        
        if ts1 == ts2:
            return c1>c2
        else:
            return ts1>ts2
        
    
    def parse_index_for_range(self,id):

        if id == '-':
            return 0
            
        if id == '+':
            return len(self.data)-1
        
        idx = self.data_idx.bisect_left(id)
        if idx!=-1:
            return idx
        return None
        
        
        
    def generate_id(self, id:str = '*'):
        count = 0
        if id == '*':
            ts = int(time.time()*1000)
            if self.last_timestamp is None or self.last_timestamp < ts:
                self.last_timestamp = ts
                return f'{ts}-{count}'

            elif self.last_timestamp == ts:
                last_ts, last_id = map(int, list(self.data.keys())[-1].split('-'))
                if last_ts == ts:
                    count = last_id + 1
                    self.last_timestamp = ts
                    return f'{ts}-{count}'
                
            elif self.last_timestamp > ts:
                last_ts, last_id = map(int, list(self.data.keys())[-1].split('-'))
                self.last_timestamp = ts
                return f'{ts}-{count}' 
            
            else:
                raise ValueError("Error While generating ID")
        
        
        if not re.match(r'^\d+-\d+$', id):
            raise ValueError("Invalid stream ID format")
        
        last_id = list(self.data.keys())[-1]
        valid = STREAM.compare_ids(id,last_id)
        
        if not valid:
            raise ValueError("Invalid stream ID")
        
        
        
        return id
    
    
    
    def xadd(self, Fields:List, id = '*',maxLen = None, Approx = False):

                 
            try:
                id = self.generate_id(id)
            except:
                return None
            
            if len(Fields)%2!=0:
                return None
            
            data = self.data.get(id,{})
            for i in range(0,len(Fields)-1,2):
                data[Fields[i]] = Fields[i+1]
            
            self.data[id] = data
            
            self.data_idx.add(id)
            self.trim(maxLen,Approx)
            
            return id
    
    def xrange(self, start = '-', end = '+', count = None):

            start_index = self.parse_index_for_range(start)
            end_index = self.parse_index_for_range(end)
            
            if start_index is None or end_index is None:
                return None
            
            if start_index>end_index:
                return None
            
            range = end_index-start_index+1
            if count and count<range:
                range = count
                
            
            response = []
            data_to_return = list(self.data.items())[start_index:start_index+range]
            for data in data_to_return:
                response.append([data[0], [x for pair in data[1].items() for x in pair]])
            return response
        
        
    def xread(self, start = '0', count = None):

            if start == '0':
                start = '-'
            
            start_index = self.parse_index_for_range(start)
        
            if start_index is None:
                return None
            
            start_index += 1
            
            range = len(self.data)-start_index
            if count and count>0:
                range = count
                
            response = []
            data_to_return = list(self.data.items())[start_index:start_index+range]
            for data in data_to_return:
                response.append([data[0], [x for pair in data[1].items() for x in pair]])
            return response
        
    
    def xdel(self, ids):

            flag = 1
            for id in ids:
                if id in self.data.keys():
                    del self.data[id]
                    self.data_idx.remove(id)
                else:
                    flag = 0
            
            return flag
        
    
    def xlen(self):
        return len(self.data)
    
        
    
    def trim(self,maxLen,Approx):
            if maxLen is not None:
                extras = len(self.data)-maxLen+1
                if extras >0:
                    trim = extras
                    if  Approx:
                        trim = int(0.2*extras) if extras <100 else int(0.5*extras)
                        
                    self.data_idx = SortedList(self.data_idx[trim:])
                    self.data = dict(list(self.data.items())[trim:])
            
            
            
            
        
            
            
            
        
        
            
        
    

            
        
        
        
        
            
            
        
    
    