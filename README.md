# couchapp-utils: Making CouchApp development a little more relaxing.

CouchApps are web applications that run entirely on a CouchDB server. The [CouchApp](https://github.com/couchapp/couchapp) scripts make it easy to create and deploy these applications.

This project is aimed at creating some additional scripts that work along side couchapp to make application development process even easier.

This is really just a sandbox for testing out some ideas. Hopefully, any scripts that prove really usefull can be integrated back into CouchApp itself.

### A proxy server for client side development

For purely client side application development, its tedious to constantly run:
    $ couchapp push

In this case it would be more convenient to simple serve all the static files from the local filesystem and proxy the dymanic URLs to the CouchDB test server.

    $ couchapp runproxy [OPTIONS] [CONFIG_JSON] 

Now the application can be accessed via the proxy port (defaults to 8000) and changes to the static files will automatically show up with just a page refresh.

After some relaxing client side coding is complete

    $ couchapp push

will push all the changes to CouchDB server and the app can be accessed without the proxy.

### Attachments to regular documents

The proxy server will also serve attachments to regular documents from the local file if they are defined in the couchapp directory under the _docs folder.

This makes it easy to have a single database of common static files to be shared amoung several couchapps. Using regular documents instead of couchapp's default behavior of attaching to design documents allows any URL path to be used for the static resources (without requiring a path entry for the design document itself).
    
    - db_dir\_docs\js\_attachments\... ==> db_name/js/...
    - db_dir\_docs\css\_attachments\... ==> db_name/css/...
    

### Server side couchapp developement

For now the normal couchapp push utility will be needed to update the design document itself 

### Installation

The easiest installation is to put runproxy.py and server.json one directory above the couchapps to be proxied. You can then run the script from that directory or from within a couchapp. The config file should be found either way.

The runproxy.py script can also be installed as a couchapp extension module so that it can be called like any other couchapp command:
    - copy runproxy.py to the couchapp/ext directory
    - add the line "runproxy=couchapp.ext.runproxy" to entry_points.txt in couchapp/EGG-INFO
    - copy the file .couchapp.conf to ~/.couchapp.conf or modify your existing one

### License

couchapp-utils is licensed the same as CouchApp which is the [Apache 2.0 License](http://www.apache.org/licenses/LICENSE-2.0)

