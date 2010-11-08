import mimetypes
import json
import os
import traceback
from Queue import Queue

import wsgiserver
import httpclient
import re
import logging

class CouchDBProxy(object):
    def __init__(self, config_file, port):
        self.config = {}
        self.port = port
        with open(config_file) as f:
            self.config = json.load(f)

        self.path_cache = {}
        self.file_cache = {}
        self.conn_queue = Queue()
        self.enable_cache = True
        self.server_name = "localhost"
        
        self.server = wsgiserver.CherryPyWSGIServer(
                ('0.0.0.0', self.port), self.proxy_server,
                server_name=self.server_name)
        
    def run(self):
        try:
            logging.info("Starting proxy server on port %s..." % self.port)
            mimetypes.init()
            self.server.start()
        except KeyboardInterrupt:
            self.server.stop()
            logging.info("Proxy server Stopped.")

    def proxy_server(self, environ, start_response):
        path = environ['PATH_INFO'].lower()
    
        # Get a proxy url or local filepath for the current requested url
        url = self.path_cache.get(path, None)
        if url is None:
            url = self.path_cache[path] = self.find_url(path)
            
        if re.match("^https?://", url):
            return self.handle_remote(environ, start_response, url)
            
        if environ["REQUEST_METHOD"] not in ["GET", "HEAD"]:
            return self.handle_remote(environ, start_response, self.force_remote(path))
        
        if os.path.exists(url):
            return self.handle_file(environ, start_response, url)
        else:
            return self.file_not_found(environ, start_response)
        
    def force_remote(self, path):
        return self.config.get("remote", "http://localhost:5984") + path
        
    def find_url(self, path):
        for app in self.config["apps"]:
            if path.startswith(app["path"]):
                not_matched = path[len(app["path"]):]
                
                # we only server attachments, not the design document itself or any other API url
                if app.get("src") and re.match("^%s/[^_]+.*" % app["path"], path):
                    return os.path.join(os.getcwd(), app["src"], *not_matched.split("/"))
                    
                result = app["url"] + not_matched
                if result.endswith("_utils"):
                    result += "/"
                
                return result

        return None

    def handle_remote(self, environ, start_response, remote_path=None):
        query = environ["QUERY_STRING"] 
        if query != "":
            remote_path = "%s?%s" % (remote_path, query)

        logging.info("%s %s" % (environ["REQUEST_METHOD"], remote_path))
        
        # we use a shared queue to try to use keep-alive connections to the
        # database for low latency
        try:
            h = self.conn_queue.get_nowait()
        except:
            h = httpclient.Http()
            
        # copy the headers from the current request into the proxied request
        headers = {}
        for key in environ.keys():
            value = environ[key]
            
            # the wsgi server's header parser appends HTTP_ to the actual request headers
            if key.startswith("HTTP_"):
                header_key = "-".join([x.lower().title() for x in key[5:].split("_")])
                headers[header_key] = environ[key]
            
            # except these two headers are renamed if the exist
            if key in ["CONTENT_TYPE", "CONTENT_LENGTH"]:
                header_key = "-".join([x.lower().title() for x in key.split("_")])
                headers[header_key] = environ[key]
            
        method = environ['REQUEST_METHOD']
        
        content_length = int(environ.get("CONTENT_LENGTH", "0"))
        if content_length > 0:
            body = environ["wsgi.input"].read( content_length )
            headers["Content-Length"] = str(len(body))
        else:
            body = ""
            
        # Try at least twice to complete a proxy before giving up
        try:
            resp, content = h.request(remote_path, method=method, headers = headers, body=body)
        except:
            # On the first error assume something was wrong with the connection
            try:
                h = httpclient.Http()
                resp, content = h.request(remote_path, method=method, headers = headers, body=body)
                logging.error(traceback.format_exc())
            except:
                # On the second error, just bailout but return a JSON encoded object since
                # couchapp javascript will be expecting that
                logging.error(traceback.format_exc())
                return handle_error(environ, start_response, "proxy_failed")
        
        # put http connection back into pool so we can use it on another request
        self.conn_queue.put_nowait(h)
        
        # copy the headers in the proxied response to the current response
        headers = []
            
        for key in resp.keys():
            if key.lower() in ["status"]:
                continue
            header_key = "-".join([x.title() for x in key.split("-")])
            headers.append( (header_key, resp[key]) )
        
        start_response("%s %s" % (resp.status, resp.reason), headers)
        return [content,]

    def handle_file(self, environ, start_response, file_path):
        curr_modified = os.path.getmtime(file_path)
        
        logging.info("%s %s" % (environ["REQUEST_METHOD"], file_path))
        
        # We keep an in-memory cache of files and only read from disk if file has been updated
        if file_path in self.file_cache:
            mimetype, encoding, last_modified, file_contents = self.file_cache[file_path]
            if last_modified != curr_modified:
                file_contents = None
        else:
            mimetype, encoding = mimetypes.guess_type(file_path)
            last_modified = None
            file_contents = None
        
        # If we don't have the most recent copy of the file in memory then read it from the disk
        if file_contents == None:
            # we read text type files in text mode, otherwise we use binary mode
            if mimetype != None and mimetype.startswith("text/"):
                with open(file_path, "rt") as f:
                    file_contents = f.read()
            else:
                with open(file_path, "rb") as f:
                    file_contents = f.read()
                    
            # If the file cache is enabled then we store the entire file contents, otherwise we
            # only store the mime type and encoding information
            if self.enable_cache:
                self.file_cache[file_path] = mimetype, encoding, curr_modified, file_contents
            else:
                self.file_cache[file_path] = mimetype, encoding, curr_modified, None

        # Write headers and start response
        headers = [ ("Cache-Control", "must-revalidate")]
        if mimetype:
            headers.append( ("Content-Type", mimetype))
        if encoding:
            headers.append( ("Content-Encoding", encoding))
            
        # TODO: we should simulate the ETAG header that CouchDB will use
        
        size = len(file_contents)
        headers.append( ("Content-Length", str(size)) )
        start_response("200 OK", headers)
        
        # This is either a GET or a HEAD request since we send everything else to CouchDB anyway
        if environ["REQUEST_METHOD"] == "HEAD":
            return "";
        else:
            return [file_contents,]
            
       
    def handle_error(self, environ, start_response, reason=None):
        if reason is None:
            "unknown_error"
            
        result = {"error" : reason, "reason" : traceback.format_exc()}
        body = json.dumps(result)
        headers = [ ("Content-Length" , str(len(body))),
            ("Content-Type" , "text/plain"),
            ("Cache-Control" , "must-revalidate") ]
        
        start_response("500 Internal Error", headers)
        return [body,]
        
    def file_not_found(self, environ, start_response, reason=None):
        logging.info("Not Found: %s" % environ["PATH_INFO"])
        if not reason:
            reason = "Document is missing attachment"
        result = {"error":"not_found",
                  "reason" : reason }
        body = json.dumps(result)
        headers = [ ("Content-Length" , str(len(body))),
                    ("Content-Type" , "text/plain"),
                    ("Cache-Control" , "must-revalidate") ]
        
        start_response("404 Object Not Found", headers)
        return [body,]

  
if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    proxy = CouchDBProxy("server.json", 8000)
    proxy.run()
    