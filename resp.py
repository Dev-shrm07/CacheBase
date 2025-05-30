
class RESP_PARSER:

    @staticmethod
    def parse(data: bytes) -> list:
        if not data:
            return []
        try:
            text = data.decode('utf-8', errors='ignore').strip()
            if not text:
                return []
            lines = text.split('\r\n')
            if lines[0].startswith('*'):
                result, _ = RESP_PARSER._parse_array_recursive(lines, 0)
                return result
            elif lines[0].startswith('+') or lines[0].startswith('-'):
                return [RESP_PARSER.process_str(lines[0][1:])]
            elif lines[0].startswith(':'):
                return [int(lines[0][1:])]
            elif lines[0].startswith('$'):
                return [lines[1] if len(lines) > 1 else None]
            else:
                return RESP_PARSER.process_array(text.split())
        except Exception:
            try:
                return data.decode('utf-8').strip().split()
            except:
                return []

    @staticmethod
    def _parse_array_recursive(lines: list, index: int) -> tuple[list, int]:
        if not lines[index].startswith('*'):
            return [], index

        try:
            count = int(lines[index][1:])
            index += 1
            result = []

            for _ in range(count):
                if index >= len(lines):
                    break

                prefix = lines[index][0]
                if prefix == '*':
                    nested, index = RESP_PARSER._parse_array_recursive(lines, index)
                    result.append(nested)
                elif prefix == '$':
                    length = int(lines[index][1:])
                    index += 1
                    if length == -1:
                        result.append(None)
                    else:
                        result.append(lines[index])
                    index += 1
                elif prefix == ':':
                    result.append(int(lines[index][1:]))
                    index += 1
                elif prefix == '+':
                    result.append(lines[index][1:])
                    index += 1
                elif prefix == '-':
                    result.append(lines[index][1:])
                    index += 1
                else:
                    result.append(lines[index])
                    index += 1
            return result, index
        except:
            return [], index

    @staticmethod
    def encode_array(obj: list) -> str:
        result = f"*{len(obj)}\r\n"
        for item in obj:
            if isinstance(item, list):
                result += RESP_PARSER.encode_array(item)
            else:
                result += f"${len(str(item))}\r\n{item}\r\n"
        return result

    @staticmethod
    def encode(obj: any) -> bytes:
        if obj is None:
            return b"$-1\r\n"
        if isinstance(obj, int):
            return f":{obj}\r\n".encode('utf-8')
        elif isinstance(obj, str):
            if obj.isdigit():
                return f":{obj}\r\n".encode('utf-8')
            if obj.startswith('ERROR'):
                return f"-{obj}\r\n".encode('utf-8')
            if obj in ['PONG', 'OK']:
                return f"+{obj}\r\n".encode('utf-8')
            return f"${len(obj)}\r\n{obj}\r\n".encode('utf-8')
        elif isinstance(obj, list):
            return RESP_PARSER.encode_array(obj).encode('utf-8')
        else:
            try:
                s = str(obj)
                return f"${len(s)}\r\n{s}\r\n".encode('utf-8')
            except:
                return b"$-1\r\n"

    @staticmethod
    def process_str(val: str):
        if val.isdigit():
            return int(val)
        try:
            num = float(val)
            return num
        except:
            return val

    @staticmethod
    def process_array(data: list):
        return [RESP_PARSER.process_str(i) for i in data]