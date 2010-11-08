from optparse import OptionParser
from proxy import CouchDBProxy
import logging

def main():
    parser = OptionParser(usage = "usage: %prog [options] config_file")
    parser.add_option("-p", "--port", dest="port", type="int",
                      metavar = "PORT",
                      help="local PORT to listen on", default=8000)
    
    config_file = "server.json"
    (options, args) = parser.parse_args()
    if len(args) >= 1:
        config_file = args[0]
    
    proxy = CouchDBProxy(config_file, options.port)
    proxy.run()
    
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    main()
    
