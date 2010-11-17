"""
runproxy

An asyncore based http proxy server for couchapps. Supports transparently serving static files in a
couchapp directly from the disk, while proxying all other requests to a CouchDB server. The proxy automatically
rewrites urls back to the couchapp directory layout.

The proxy is designed to support keep-alive connections, chunked encoding and pipelining.
Eventually, it should also support HTTPS connections on either (or both) ends.

This file can be used as a couchapp extension module by dropping it in the ext directory of
couchapp, or it can be run directly from the command line.

Configuration File
---------------------------------------------------------------------------------------------------
A JSON formatted configuration file should be specified to control the behaviour of the proxy server.
The following top level fields can be specified:
    - apps
       - a dictionary of directory to database name mappings
    - urls
       - a list of regular expressions that map URLs to file directories
         (a default list will be generated at startup by walking the couchapp directories)
    - options
       - a dictionary of options that can override the default parameters
         for serving local files
    - remote_scheme
       - http or https (currently has no affect)
    - remote_host
       - the hostname for the couchdb server (default: 127.0.0.1)
    - remote_port
       - the port for the couchdb server (default: 5984)


Asyncronous File Access
----------------------------------------------------------------------------------------------------
File operations are inherantly synchronous and therefore not compatible with the design philosphy
an asyncore application.

To work around this, a pool of file access threads is maintained that accept file request tasks
and return an appropriate HTTP response for the resource.

The main proxy dispatch handler decides when a requested URL should be handled by the file thread pool
or when the URL should be proxied to the remote server.

Most file operations DO release the GIL so using python threads should be fairly efficient along side
asyncore. However, even better performance on multi-core systems could be achieved by using multiprocessing
instead. This should be pretty easy to implement since we are already using a Queue object as the
internal communications mechanism between thread boundaries.

Pipelining
----------------------------------------------------------------------------------------------------
True pipelining of requests isn't performed by very many browsers right. However, HTTP/1.1
servers and proxies are supposed to support pipelining, so its been implemented in this
proxy.

This requires tracking requests and ensuring responses are returned in the proper order, which
may be non-trivial when some requests are handled as a local file and other requests are handled
as proxied remote requests.

Parsing the request from the client if necessary to break a stream of requests into individual
requests that can be served from either the local file system or proxied to the remote server.

Parsing the response from a proxied request is necessary to detect when the response is complete
(since the connection will normally remain open).

If request/response stream is significantly malformed, the parser may not detect the end criteria and
will hold the response open until a timeout on the open socket occurs.


Basic order of operations
----------------------------------------------------------------------------------------------------
1. ProxyServer dispatches each incoming connection to a new ProxyHandler
   - The ProxyHandler creates a RemoteRequestHandler which opens a new connection to the
     remote server
   - The ProxyHandler also creates its own LocalFileHandler, but this object just manages
     a little bit of state and passes all requests directly to the thread pool.
2. ProxyHandler passes incoming data thru its HttpParser which separates the inbound stream
   into discrete requests. Once a new request is identified, a ResponseBuffer object is
   created and given to the appropriate handler.
   - For GET/HEAD requests to URLs that match one of the local file paths (config['urls'])
     URL is mapped to a local file path and the LocalFileHandler is used.
   - For all other request methods and URLs the RemoteRequestHandler is used
3. LocalFileHandler waits until the entire header has been received and then passes the request
    file path, request headers and ResponseBuffer to the FileRequestThreadPool.
   - The thread pool creates a task to serve the file and places the task on a Queue that
     all inactive FileRequestThreads are blocking on
   - After receiving a task from the task queue, the FileRequestThread generates the
     appropriate HTTP Response for that request and appends it to the ResponseBuffer.
   - When the response has been completed, the FileRequestThread automatically closes the ResponseBuffer
4. RemoteRequestHandler proxies all incoming data it receives from the ProxyHandler to the
   remote server and processes the remote server's response stream thru its own HttpParser.
   - Incoming data stream is appended to the current ResponseBuffer object
   - After each complete incoming response the RemoteRequestHandler closes the current ResponseBuffer
     and begins using the next one.
5. The ProxyHandler keeps track of the order of requests and ensures that the ResponseBuffers are
   output back to the client in the same order.
6. As request handlers begin to provide response data to their ReponseBuffers, the ProxyHandler
   begins transfer of the oldest ResponseBuffer to the output. When that response is marked as complete
   and the buffer is empty, the next ResponseBuffer in the queue begins transfering to client.

"""

import asyncore
import socket
import traceback
import json
import os
import re
import urlparse
import threading
from Queue import Queue
import time
import calendar
import mimetypes
import hashlib
from email.utils import parsedate
import StringIO
import gzip
import logging

class FileRequestThreadPool(object):
    """ Manages a thread pool of workers to read files from disk and a threadsafe cache shared by all workers. """
    def __init__(self, total_threads, auto_start = True, options = None):
        self.tasks = Queue()

        self.cache = {}
        self.response_options =     {  "chunked" : True,                # Allow chunked encoding
                                        "gzip" : False,                  # Allow gzip encoding
                                        "gzip_size" : 1024 * 100,        # Minimum size of file to gzip
                                        "chunk_size" : 1024 * 8,         # Maximum chunk size
                                        "cache" : True,                  # Save previous responses in cache
                                        "etag" : True,                   # Calculate and return Etags
                                        "modified" : True,               # Enable 304 Not Modified Responses
                                        "server" : "CouchAppProxy/1.0"   # Server string
                                      }

        if options != None:
            self.response_options.update(options)

        self.cache_lock = threading.Condition(threading.Lock())
        self.joining = False
        self.threads = [FileRequestThread(self) for i in range(total_threads)]

        mimetypes.init()

        if auto_start:
            self.start_all()

    def start_all(self):
        # Start all the threads as daemon processes
        for thread in self.threads:
            if not thread.started:
                thread.daemon = True
                thread.start()

        # Wait until all the threads have actually started
        done = False
        while not done:
            done = True
            for thread in self.threads:
                if not thread.started:
                    done = False

    def cache_set(self, key, head=None, value=None):
        self.cache_lock.acquire()
        try:
            old_head, old_value = {}, None
            if key in self.cache:
                old_head, old_value = self.cache[key]

            if head is None:
                head = old_head

            self.cache[key] = (head, value)
        finally:
            self.cache_lock.release()

    def cache_get(self, key):
        self.cache_lock.acquire()
        try:
            return self.cache.get(key, ({}, None))
        finally:
            self.cache_lock.release()

    def add_request(self, resp_buff, path, headers=None, **kw):
        if self.joining:
            return False

        if headers is None:
            headers = {}

        options = {}
        options.update(self.response_options)
        options.update(kw)

        self.tasks.put( (resp_buff, path, headers, options) )
        return True

    def stop_all(self, wait_tasks = True, wait_threads = True):
        self.joining = True

        # Wait until all the tasks have been dequeue
        if wait_tasks:
            while not self.tasks.empty():
                time.sleep(.1)

        # Set all threads to be inactive
        for thread in self.threads:
            thread.active = False

        # Add dummy tasks to queue to unblock waiting tasks
        for thread in self.threads:
            self.tasks.put( (None, None, None, None) )

        # Wait until all the threads have stopped running
        if wait_threads:
            running = False
            while True in [t.running for t in self.threads]:
                time.sleep(.1)


class FileRequestThread(threading.Thread):
    """ Waits for file request tasks and then generates the HTTP response to serve the file."""
    def __init__(self, pool):
        threading.Thread.__init__(self)
        self.pool = pool
        self.running = False
        self.active = True
        self.started = False

    def run(self):
        self.running = True
        self.started = True

        while self.active:
            self.check_task()
        self.running = False

    def json_error(self, error, reason):
        return '{"error":"%s","reason":"%s"}' % (error, reason)

    def check_task(self):
        resp_buff, path, headers, options = self.pool.tasks.get()
        if resp_buff == None:
            self.active = False
        else:
            self.process_request(resp_buff, path, headers, options)
            assert resp_buff.complete, "Should be complete"


    def send_response(self, handler, status, headers, content=None, close_response=False):
        """ Send a http response to the request handler """
        if content != None and "Content-Length" not in headers and headers.get("Transfer-Encoding","") != "chunked":
            headers['Content-Length'] = str( len(content) )

        message = ["HTTP/1.1 %s" % status]
        for key in sorted(headers.keys()):
            message.append("%s: %s" % (key, headers[key]))

        if content != None:
            message.append("")
            message.append(content)
        else:
            message.append("\r\n")

        handler.append("\r\n".join(message))
        if close_response:
            handler.close()

    def process_request(self, handler, path, headers, options):
        """ Reads a file from disk and sends an HTTP response to handler. """

        # Setup the default response headers
        response = { "Server" : options["server"],
                     "Date" : self.date_time_string( options.get("timestamp")),
                     "Cache-Control" : "must-revalidate"
                    }

        # Make sure the file exists and that its not a directory
        if not os.path.exists(path) or not os.path.isfile(path):
            response["Content-Type"]  = "text/plain;charset=utf-8"
            self.send_response(handler, "404 Object Not Found", response,
                               self.json_error("not_found", "Document is missing attachement"), True)
            return False

        cache_header, cache_content = self.pool.cache_get(path)

        # Get the mime type from the cache header or from the file extension
        if "mimetype" not in cache_header:
            cache_header["mimetype"] = mimetypes.guess_type(path)

        mime_type, mime_encoding = cache_header.get("mimetype", (None, None))
        cache_etag = cache_header.get("etag")
        cache_encoding = cache_header.get("encoding")
        if mime_type is None:
            mime_type = "application/octet-stream"

        # Get the time the file was last modified (clear cache contents if too old)
        last_modified = os.path.getmtime(path)
        if 'last_modified' in cache_header and cache_header['last_modified'] < last_modified:
            cache_content = None
            cache_encoding = None
            cache_etag = None
            mime_type, mime_encoding = mimetypes.guess_type(path)

        if not options['etag']:
            cache_etag = None

        # Check if we should return a 403 Not Modified response
        if options["modified"]:
            if cache_etag == headers.get("IF-NONE-MATCH", ""):
                response["Etag"] = cache_etag
                self.send_response(handler, "304 Not Modified", response, "", True)
                return True

            if "IF-MODIFIED-SINCE" in headers:
                try:
                    # Convert a parsed time in GMT back to a timestamp
                    modified_since = headers["IF-MODIFIED-SINCE"].strip()
                    if modified_since.endswith("GMT"):
                        req_modified = calendar.timegm(parsedate(modified_since))
                    else:
                        # TODO: handle timestampts that aren't in GMT
                        req_modified = 0
                except:
                    print "Error Parsing: %s" % headers["IF-MODIFIED-SINCE"]
                    traceback.print_exc()
                else:
                    if req_modified > last_modified:
                        if cache_etag != None:
                            response["Etag"] = cache_etag
                        self.send_response( handler, "304 Not Modified", response, "", True)
                        return True

        # Add headers from cached info
        if mime_type != None:
            response["Content-Type"] = mime_type
        if mime_encoding != None:
            response["Content-Encoding"] = mime_encoding
        if cache_etag != None:
            response["Etag"] = cache_etag
        if cache_encoding != None:
            response["Transfer-Encoding"] = cache_encoding

        # Check if we should just return the cached response
        if cache_content != None:
            self.send_response( handler, "200 OK", response, cache_content, True)
            return True

        # Check if we should gzip the file first
        if options['gzip'] and mime_encoding != "gzip" and os.path.getsize(path) > options['gzip_size']:
            mime_encoding = response["Content-Encoding"] = "gzip"
            buffer = StringIO.StringIO()
            zf = gzip.GzipFile(fileobj=buffer, mode="wb")
            try:
                with open(path, "rb") as f:
                    zf.write(f.read())
            finally:
                zf.close()

            buffer.seek(0)
        else:
            mime_encoding = response["Content-Encoding"] = "identity"
            if mime_type.startswith("text/"):
                buffer = open(path, "rt")
            else:
                buffer = open(path, "rb")

        # Read file contents and generate http response with possible chunking
        chunk_size = options['chunk_size']
        try:
            # First try to read a chunk-size and see if we get everything
            if options["chunked"]:
                content = buffer.read(chunk_size)
            else:
                content = buffer.read()

            last_read = len(content)
            if options["chunked"] and last_read >= chunk_size:
                cache_encoding = response["Transfer-Encoding"] = "chunked"
                content = "\r\n".join( ["%X" % last_read, content, ""] )
                total = [content]
                self.send_response(handler, "200 OK", response, content, False)

                while last_read == chunk_size:
                    content = buffer.read(chunk_size)
                    last_read = len(content)
                    content = "\r\n".join( ["%X" % last_read, content, ""] )
                    handler.append(content)
                    total.append(content)

                total.append("\r\n".join(["0","\r\n"]))
                cache_content = "".join(total)

                if options["etag"]:
                    cache_etag = self.get_etag(cache_content)
                ending= ["0"]
                if cache_etag != None:
                    ending.append("Etag: %s" % cache_etag)
                ending.append("\r\n")
                content = "\r\n".join( ending )
                handler.append(content)
                handler.close()
            else:
                if options["etag"]:
                    cache_etag = self.get_etag(content)
                    response["Etag"] = cache_etag
                cache_content = content
                self.send_response(handler, "200 OK", response, content, True)
        except:
            traceback.print_exc()
            handler.close()

        finally:
            buffer.close()

        # If the cache options flag is not set then we don't save the contents of the
        # file only the header info
        if not options['cache']:
            cache_content = None

        cache_header["mimetype"] = mime_type, mime_encoding
        cache_header["etag"] = cache_etag
        cache_header["encoding"] = cache_encoding
        cache_header['last_modified'] = last_modified
        self.pool.cache_set(path, cache_header, cache_content)
        return True

    def get_etag(self, content):
        return '"%s"' % hashlib.sha1(content).hexdigest()

    # Copied from BaseHTTPServer.py
    def date_time_string(self, timestamp=None):
        """Return the current date and time formatted for a message header."""
        if timestamp is None:
            timestamp = time.time()
        year, month, day, hh, mm, ss, wd, y, z = time.gmtime(timestamp)
        s = "%s, %02d %3s %4d %02d:%02d:%02d GMT" % (
                self.weekdayname[wd],
                day, self.monthname[month], year,
                hh, mm, ss)
        return s

    weekdayname = ['Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat', 'Sun']

    monthname = [None,
                 'Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun',
                 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']

class HttpParserResult(object):
    def __init__(self, data, header_line, headers, complete):
        self.data = data
        self.header_line = header_line
        self.headers = headers
        self.complete = complete

class HttpParser(object):
    """ A utility object that can incrementally parse an HTTP request or response.

    Supports chunked transport encoding. Also supports pipelining with multiple
    requests or responses in a single data stream. Incremental parsing allows data stream to break
    anywhere and results indicate if they are partial or complete.
    """

    def __init__(self, header_line):
        self.buffer = []
        self.headers = {}
        self.header_complete = False
        self.body_complete = False
        self.complete = True
        self.re_header = re.compile(header_line)
        self.re_hexnum = re.compile("([A-F0-9]+)")
        self.body_left = 0
        self.last_data = ""
        self.results = []
        self.header_line = None
        self.first_line = True

    def push_results(self):
        result = HttpParserResult("".join(self.buffer), self.header_line, self.headers, self.complete)
        if not (self.header_complete or self.body_complete):
            result.headers = None
        self.results.append(result)
        self.buffer[:] = []

    def read(self, data):
        """ Reads a block of data that might contain multiple pipelined requests.

        Returns a list of (data, headers, complete_flag) for each request found
        """

        self.results = []
        if len(self.last_data) > 0:
            data = self.last_data + data
            self.last_data = ""

        self.append(data)

        if not self.complete:
            self.complete = self.body_complete and self.header_complete
            self.push_results()

        # Only the last response or request in the list should be partial
        for obj in self.results[:-1]:
            if not obj.complete:
                print "Incomplete Result: %s" % obj.header_line
            obj.complete = True

        return self.results

    def append(self, data):
        """ Recursively walks thru a block of data updating the state of the parser. """
        if len(data) == 0:
            return

        if self.complete:

            m = self.re_header.search(data)
            if m and "\r\n" in data:
                # discard anything between end of last item and beginning of next
                data = data[m.start():]

                self.complete = self.body_complete = self.header_complete = False
                self.headers = {}
                self.buffer = []
                self.header_line = m.groups()
                self.first_line = True
            else:
                # accumulate input until we see the next header line
                self.last_data = self.last_data + data
                return

        # Strip out the header lines until an empty header is found
        while not self.header_complete and len(data) > 0:
            if "\r\n" in data:
                header, data = data.split("\r\n", 1)

                if header == "":
                    self.header_complete = True
                    self.body_left = int(self.headers.get("CONTENT-LENGTH", "0"))
                    self.chunked = self.headers.get("TRANSFER-ENCODING", "") == "chunked"
                    if not self.chunked and self.body_left == 0:
                        self.body_complete = True
                else:
                    self.buffer.append(header)
                    if self.first_line:
                        self.first_line = False
                    elif ":" in header:
                        header, value = header.split(":", 1)
                        self.headers[header.strip().upper()] = value.strip()

                self.buffer.append("\r\n")
            else:
                self.last_data = data
                return

        # Check for completion
        self.complete = self.header_complete and self.body_complete
        if self.complete:
            self.push_results()
            return self.append(data)

        # Process as much body content as possible
        while not self.body_complete and len(data) > 0:
            if self.body_left > 0:
                if len(data) >= self.body_left:
                    self.buffer.append(data[:self.body_left])
                    data = data[self.body_left:]
                    self.body_left = 0
                else:
                    self.buffer.append(data)
                    self.body_left -= len(data)
                    data = ""

            if self.body_left == 0:
                if self.chunked:
                    if "\r\n" in data:
                        size_line, data = data.split("\r\n", 1)

                        if size_line != "":
                            self.buffer.append(size_line)
                            self.buffer.append("\r\n")
                            m = self.re_hexnum.search(size_line.upper())
                            if m:
                                self.body_left = int(m.groups()[0], 16)
                                if self.body_left == 0:
                                    self.body_complete = True
                                    self.header_complete = False
                                    return self.append(data)
                        else:
                            self.buffer.append("\r\n")
                    else:
                        self.last_data = data
                        return
                else:
                    self.body_complete = True

        # Check if complete
        self.complete = self.header_complete and self.body_complete
        if self.complete:
            self.push_results()

        if len(data) > 0:
            return self.append(data)

class HttpRequestParser(HttpParser):
    def __init__(self):
        HttpParser.__init__(self, "([A-Z]+) (\S+?) (HTTP\S+)")

class HttpResponseParser(HttpParser):
    def __init__(self):
        HttpParser.__init__(self, "(HTTP\S+) (\d+)")

class LocalFileHandler(object):
    """ Keeps track of new seq_id, waits for completed headers and sends a request to thread pool. """
    def __init__(self, handler, pool):
        self.pool = pool
        self.handler = handler
        self.resp_buff = None

    def start_request(self, resp_buff, method, path, version):
        if self.resp_buff != None:
            self.resp_buff.close()

        self.resp_buff = resp_buff
        self.method = method
        self.path = path
        self.version = version
        self.headers = None

    def set_headers(self, headers):
        if self.headers == None and headers != None:
            self.headers = headers
            # Send the complete request with headers to the file request thread pool
            self.pool.add_request(self.resp_buff, self.path, self.headers)
            self.resp_buff = None

    def append_request(self, data):
        pass

class RemoteRequestHandler(asyncore.dispatcher):
    """ Manages a connection to a remote server.

        Keeps a queue of seq_id for each request passed to it, while transparently proxying request data stream
        to the server.

        Reponse data stream from server is parsed into individual requests, associated with the next
        available seq_id and passed to the ProxyHandler.
    """
    def __init__(self, handler, address):
        asyncore.dispatcher.__init__(self)
        self.buffer = []
        self.delay_buffer = []

        self.closing = False

        self.handler = handler
        self.address = address

        self.create_socket (socket.AF_INET, socket.SOCK_STREAM)
        self.connect (address)
        self.response_queue = []

        self.response_parser = HttpResponseParser()
        self.wait_buffer = []

    # Asyncore dispatcher members

    def handle_connect (self):
        if not self.handler.server.quiet:
            print '[%s] Connected to %s' % (self.id, repr(self.address))

    def writable(self):
        return (len(self.buffer) > 0)

    def handle_write(self):
        # try to send everything in the buffer
        line = "".join(self.buffer)
        sent = self.send(line)
        line_sent = line[:sent]

        # put anything that wasn't sent back on the buffer for next time
        line = line[sent:]
        self.buffer = []
        if len(line) > 0:
            self.buffer.append(line)

    def dispatch_buffer(self):
        # Check if there is a request object yet to dump data into
        if len(self.response_queue) == 0:
            return

        # Get the data waiting to be processed
        data = "".join(self.wait_buffer)
        self.wait_buffer[:] = []

        # Get all the results from incrementally incoming data stream
        results = self.response_parser.read( data )

        for i, result in enumerate(results):
            if len(self.response_queue) == 0:
                print "ERROR: Got response data with nowhere to put it!"
                self.wait_buffer.append(result.data)

            resp = self.response_queue[0]

            # If we get to the results of a HEAD request, the state of the
            # parser is probably messed up if the client tries to pipeline
            # a request after the HEAD request. Since the response parser
            # doesn't know what kind of requests it is parsing, it won't
            # know to ignore the content-length field.
            #
            # The parser could be modified to abort processing if the line
            # following a complete header is another HTTP response line
            # but I'm not sure this is correct behavior.
            #
            # So for now we let the parser get messed up and triage the
            # results and parser state when we notice the response was for
            # a HEAD request.
            #
            if resp.method == "HEAD" and result.headers != None:
                header, body = result.data.split("\r\n\r\n")

                # The HEAD request is complete so push it to the response buffer
                resp.append(header + "\r\n\r\n")
                resp.close()
                self.response_queue.remove(resp)


                # Now we have to fix potential errors in the parsing state
                # The rsponse body of a HEAD request is empty so this is probably
                # the next response


                if len(body) > 0:
                    print "Fix HEAD: returning extra: %d" % len(body)
                    self.wait_buffer.append(body)


                # All remaining pipelined responses are probably messed up so put
                # content back on the inbound queue
                for j in range(i+1, len(results)):
                    print "Fix HEAD: return Pipelined: %d" % j
                    self.wait_buffer.append(results[j].data)


                # There also might be a little data that wasn't able to be fully parsed
                # depending on where the read buffer got cut off
                if len(self.response_parser.last_data) > 0:
                    print "Fix HEAD: return unparsed: %d" % len(self.response_parser.last_data)
                    self.wait_buffer.append(self.response_parser.last_data)
                    self.response_parser.last_data = ""

                # Set the parser back to the completed state so it'll look for a new
                # response line
                self.response_parser.complete = True

                # Reprocess any data we put back onto the inbound queue
                if len(self.wait_buffer) != 0:
                    return self.dispatch_buffer()
                else:
                    return

            resp.append(result.data)
            if result.complete:
                resp.close()
                self.response_queue.remove(resp)


    def handle_read(self):
        self.wait_buffer.append(self.recv(8192))
        self.dispatch_buffer()

    def handle_close(self):
        if not self.handler.server.quiet:
            print "[%s] Closing (Remote)" % self.id
        for resp in self.response_queue:
            resp.close()

        self.close();
        self.handler.handle_close()

    def handle_error(self):
        print "[%s] Error (Remote): %s" % (self.id, traceback.format_exc())
        for resp in self.response_queue:
            resp.close()

    # Request Handler methods
    def start_request(self, resp_buff, method, path, version):
        self.response_queue.append(resp_buff)
        self.dispatch_buffer()

    def append_request(self, data):
        """ proxies incoming request data to the write buffer. """
        self.buffer.append(data)

    def set_headers(self, headers):
        pass

class ResponseBuffer(object):
    """ A buffer to hold HTTP responses and completion status.

    The write() and append() methods are thread safe.
    However, write() and unwrite() should be called by the same thread.
    """

    def __init__(self, seq_id):
        self.seq_id = seq_id
        self.last_buffer = ""
        self.queue = Queue()
        self.complete = False
        self.create_time = time.time()
        self.start_time = None
        self.method = None
        self.url = None

    def write(self):
        """ Returns the entire contents of the buffer as a single string. """
        buffer = [self.last_buffer]
        self.last_buffer = ""
        while not self.queue.empty():
            try:
                buffer.append(self.queue.get_nowait())
            except:
                pass

        return "".join(buffer)

    def unwrite(self, data):
        """ Allows previously retrieved data to be put back at the begining of the buffer. """
        self.last_buffer = data

    def close(self):
        self.complete = True
        self.close_time = time.time()

    def append(self, data):
        if self.start_time == None:
            self.start_time = time.time()

        if len(data) != 0:
            self.queue.put_nowait(data)

    def writable(self):
        return (not self.queue.empty() or len(self.last_buffer) > 0)

class ProxyHandler (asyncore.dispatcher):
    """ Manages a connection to the client.

    Processes the inbound request stream for individual requests. Generates a new
    seq_id for each request and passes the request stream off to the appropriate
    request dispatch handler LocalFileHandler or RemoteRequestHandler.

    Inbound response streams from the request dispatch handlers are associated with
    the seq_id of the request and buffered if required to maintain proper response
    order.

    As response bufferes are available, data is transfered back to the client in the
    same order that requests were received.
    """

    def __init__ (self, server, conn):
        if conn != None:
            asyncore.dispatcher.__init__ (self, conn)

        self.remote_handler = RemoteRequestHandler(self, server.remote_addr)
        self.file_handler = LocalFileHandler(self, server.thread_pool)
        self.handler = None
        self.server = server

        self.id = self.server.counter
        self.server.counter = self.server.counter + 1

        self.remote_handler.id = self.id
        self.file_handler.id = self.id

        self.seq_id = 0

        self.request_parser = HttpRequestParser()

        self.response_queue = []    # a queue of response buffer objects
        self.closing = False        # a flag used during closing to prevent loops

    def writable(self):
        return True
        # try:
        #     if len(self.response_queue) == 0:
        #         return False
        #     return self.response_queue[0].writable()
        # except:
        #     return False

    def handle_read(self):
        # The HttpParser will parse the entire data stream sent to it into individual requests
        # which might be partial requests.
        for result in self.request_parser.read( self.recv(8192) ):
            # If the current handler is set then we are parsing a partial result.
            if self.handler is None:
                self.start_request(result.header_line)

            # Forward data stream and parsed header to the current handler
            if len(result.data) > 0:
                self.handler.append_request(result.data)
            if result.headers != None:
                self.handler.set_headers(result.headers)

            # Ensure that a new request will be started on next iteration
            if result.complete:
                self.handler = None

    def handle_write(self):
        if len(self.response_queue) == 0:
            return

        if not (self.response_queue[0].writable()):
            return

        # Get the top reponse buffer in the queue
        resp = self.response_queue[0]

        line = resp.write()
        sent = self.send(line)

        resp.unwrite(line[sent:])


        if resp.complete and not resp.writable():
            #t1 = time.time()
            #print "[%s:%s] S: %0.4fsec C: %0.4fsec H: %0.4fsec" % (self.id, resp.seq_id,
            #            resp.start_time - resp.create_time,
            #            resp.close_time - resp.create_time,
            #            t1-resp.create_time)

            self.response_queue.remove(resp)


    def handle_close(self):
        if not self.server.quiet:
            print "[%s] Closing" % self.id

        if self.writable() and not self.closing:
            self.closing = True
            self.handle_write()
            self.closing = False

        self.remote_handler.close();
        self.close();

    def start_request(self, header_line):
        # Check if this should be a file request
        method, url, version = header_line

        url_path = url
        self.handler = self.remote_handler
        if method in ["HEAD", "GET"]:
            url_parts = urlparse.urlsplit(url)
            for _, path, re_url in self.server.config['urls']:
                m = re_url.match(url_parts.path)
                if m:
                    url_path = os.path.join(path, *m.groups()[0].split("/"))
                    self.handler = self.file_handler
                    break

        # Create a new request seq_id and add it to the buffer
        resp = ResponseBuffer(self.seq_id)
        resp.method = method
        resp.url = url
        self.response_queue.append(resp)
        if not self.server.quiet:
            print "[%s:%s] %s %s" % (self.id, self.seq_id, method, url_path)
        self.handler.start_request(resp, method, url_path, version)
        self.seq_id += 1

class ProxyServer (asyncore.dispatcher):
    def __init__ (self, config_file="server.json", port=8000, quiet=False, verbose=False, **opts):
        asyncore.dispatcher.__init__ (self)
        self.create_socket (socket.AF_INET, socket.SOCK_STREAM)
        self.set_reuse_addr()
        self.handler = ProxyHandler
        self.port = port
        self.quiet = quiet
        self.verbose = verbose

        self.bind (("0.0.0.0", port))

        self.listen (5)
        self.counter = 0

        self.config = { "remote_protocol" : "http",
                        "remote_host" : "127.0.0.1",
                        "remote_port" : 5984,
                        "apps" : {},
                        "urls" : [],
                        "options" : None}

        self.load_config(config_file)

        # Command line parameters override even the config file
        self.config.update(opts)

        self.thread_pool = FileRequestThreadPool(5, options = self.config['options'])

        self.version_string = self.config.get("version_string", "CouchAppProxy/1.0")
        self.remote_addr = (self.config['remote_host'], self.config['remote_port'])

        if not self.quiet:
            self.print_config()

    def handle_accept (self):
        try:
            pair = self.accept()
        except socket.error:
            print 'warning: server accept() threw an exception'
            return
        except TypeError:
            print 'warning: server accept() threw EWOULDBLOCK'
            return

        if pair is None:
            pass
        else:
            sock, addr = pair
            if not self.quiet:
                print '[%s] Incoming connection from %s' % (self.counter, repr(addr))
            self.handler(self, sock)

    def print_config(self):
        # Print the results
        urls = self.config['urls']
        proxy_root = self.config["proxy_root"]

        if len(urls) > 0:
            print "Local Root:\n    %s\n" % proxy_root
            print "Local URLs:"

        for url, path, r in urls:
            if path.startswith(proxy_root):
                path = "." + os.path.join(path[len(proxy_root):], "*")
            print "   /%s/* => %s" % (url.strip("/"), path)

        print "\nProxied URLs:\n    http://localhost:%s/* => %s://%s:%s/*" % (self.port, self.config["remote_protocol"], self.config["remote_host"], self.config["remote_port"])
        print "-"*80

    def load_config(self, filename):


        # its sometimes convenient to start from inside the couchapp but the
        # configuration file and proxy root should be one level higher

        proxy_root = os.getcwd()
        if os.path.exists(".couchapprc") or os.path.exists("couchapp.json"):
            proxy_root = os.path.join(proxy_root, "..")

        self.config["proxy_root"] = proxy_root

        try:
            if os.path.exists(filename):
                config_file = filename
            else:
                config_file = os.path.abspath(os.path.join(proxy_root, filename))

            if os.path.exists(config_file):
                with open(config_file, "rt") as f:
                    self.config.update(json.load(f))
        except:
            print "Error reading: %s\n%s" % (filename, traceback.format_exc())

        self.config["proxy_root"] = proxy_root = os.path.abspath(self.config["proxy_root"])

        db_dirs = {}
        attach_dirs = []

        for root, dirs, files in os.walk(proxy_root):
            if root == proxy_root:
                continue

            parent, current = os.path.split(root)

            # Find all design documents
            if parent == proxy_root:
                db_name = self.config["apps"].get(current, current)
                db_id = "_design/%s" % db_name
                if "_id" in files:
                    with open(os.path.join(root, "_id"), "rt") as f:
                        db_id = f.readlines()[0].strip()

                db_dirs[root] = [db_name, db_id]

            # Find attachement directories on the design document or on a vendor path
            if current == "_attachments":
                attach_dirs.append(root)

            # Prevent following special directories
            if current in ["_docs", "_attachments"] or current.startswith("."):
                dirs[:] = []

        urls = self.config['urls']
        # Attach the proxy root to any hand written url maps in the config file
        for item in urls:
            item[1] = os.path.abspath(os.path.join(proxy_root), item[1])
            if len(item) > 2:
                item[2] = re.compile(item[2], re.IGNORECASE)
            else:
                item[2] = re.compile(item[0], re.IGNORECASE)

        # Combind the app db directories with the attachment directories into a rewrite url
        for path in attach_dirs:
            for key in db_dirs.keys():
                if path.startswith(key):
                    no_root, _ = os.path.split(path[len(key):])
                    no_root = no_root.strip(os.path.sep)
                    parts = db_dirs[key][:]
                    if no_root != "":
                        parts.extend(no_root.split(os.path.sep))
                    url = "/".join(parts)
                    urls.append( [url, path, re.compile("/%s/([^_].*)" % url, re.IGNORECASE)] )

        # Sort urls longest to shortest so the most specific regular expressions are listed first
        self.config['urls'] = urls = sorted(urls,reverse = True)


def runproxy(*args, **opts):
    port = int(opts.get('port', 8000))
    quiet = opts.get('quiet', False)
    verbose = opts.get('verbose', False) and not quiet
    remote = opts.get('remote', None)

    config = {}
    if remote != None:
        remote = urlparse.urlparse(remote)
        if remote.hostname != None:
            config['remote_host'] = remote.hostname
            if remote.scheme != None:
                config['remote_scheme'] = remote.scheme
            if remote.port != None:
                config['remote_port'] = remote.port

    config_file = "server.json"
    if len(args) > 1:
        config_file = args[0]

    server = ProxyServer(config_file, port = port, quiet=quiet,
                                      verbose = verbose,
                                      **config)

    print "Proxy server running on port %s ... " % server.port
    try:
        asyncore.loop(timeout=2)
    except KeyboardInterrupt:
        print "Ctrl+C pressed. Shutting down."

    server.thread_pool.stop_all()

cmdtable = {
    "runproxy":
        (runproxy,
        [('p', 'port', 8000, "local PORT to proxy from"),
         ('r', 'remote', None, "url to couchdb server to proxy to"),
         ('q', 'quiet', False, "don't print console output"),
         ('v', 'verbose', False, "print extra console output"),
         ],
        "[OPTION]... [CONFIG_FILE]")
}

if __name__ == '__main__':
    from optparse import OptionParser

    parser = OptionParser(usage = "usage %prog " + cmdtable['runproxy'][2])
    for short, long, default, help in cmdtable['runproxy'][1]:
        action = {True : "store_false", False : "store_true"}.get(default, "store")
        parser.add_option("-"+short, "--"+long, dest=long, default=default,
                          action=action, help = help)

    (options, args) = parser.parse_args()

    runproxy(*args,  port = options.port,
                     quiet = options.quiet,
                     verbose = options.verbose,
                     remote = options.remote)
