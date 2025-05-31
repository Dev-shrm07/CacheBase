from server.server import CACHE_SERVER

if __name__ =="__main__":
    server = CACHE_SERVER(log_file_path='logs/app.log')
    server.start()