

class RESP_PARSER:
    """ class to parse and encode resp """

    @staticmethod
    def parse(data:bytes)->list[any]:
        if not data:
            return []
    
        try:
            text = data.decode('utf-8',errors='ignore').strip()
            if not text:
                return []
            if text.startswith('*'):
                return RESP_PARSER.parse_array(text)
            elif text.startswith('+') or text.startswith('-'):
                return [RESP_PARSER.process_str(text[1:])]
            elif text.startswith(':'):
                try:
                    value = int(text[1:])
                    return [RESP_PARSER.process_str(value)]
                except:
                    return []    
            elif text.startswith('$'):
                return RESP_PARSER.parse_bulk(text)           
            else:
                return text.split()
                       
        except Exception as e:    
            try:
                return data.decode('utf-8').strip().split()
            except:
                return []
            


    @staticmethod
    def parse_array(text:str)->list[any]:
        lines = text.split('\r\n')
        if not lines[0].startswith('*'):
            return text.split()
        try:
            count = int(lines[0][1:])
            result = []
            i = 1

            while i < len(lines) and count > 0:
               
                if not lines[i].startswith('$'):
                    count -= 1
                    result.append(RESP_PARSER.process_str(lines[i]))
                    
                i += 1
                
            return result
        
        except:
            return text.split()

    
    @staticmethod
    def parse_bulk(text:str)->list[any]:
        lines = text.split('\r\n')
        if len(lines)>=2:
            try:
                length = int(lines[0][1:]) 
                if length != -1:
                    return [RESP_PARSER.process_str(lines[1])]
                else:
                    return [None]
            
            except:
                pass

        else:
            return [RESP_PARSER.process_str(text[1:])]
        
    
    @staticmethod
    def encode(obj:any)->bytes:
        if obj is None:
            return b"$-1\r\n"
        
        if isinstance(obj,int):
            return f":{obj}\r\n".encode('utf-8')
        
        elif isinstance(obj,str):
            if obj.isdigit():
                return f":{obj}\r\n".encode('utf-8')
            
            if obj.split()[0]=='ERROR':
                return f"-{obj}\r\n".encode('utf-8')
            
            if obj in ['PONG','OK']:
                return f"+{obj}\r\n".encode('utf-8')
            
        
            return f"${len(obj)}\r\n{obj}\r\n".encode('utf-8')
        
        elif isinstance(obj,list):
            result = f"*{len(obj)}\r\n"
            for item in obj:
                length = len(str(item))
                result += f"${length}\r\n{item}\r\n"
                
            
            return result.encode('utf-8')
        else:
            try:
                s = str(obj)
                return f"${len(s)}\r\n{s}\r\n".encode('utf-8')
            
            except:
                return b"$-1\r\n"
        
        
    @staticmethod
    def process_str(val:str):
        if val.isdigit():
            return int(val)
        
        try:
            num = float(val)
            return num
        except:
            return val
            
                

                
