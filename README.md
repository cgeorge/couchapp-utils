# couchapp-utils: Making CouchApp development a little more relaxing.

CouchApps are web applications that run entirely on a CouchDB server. The [CouchApp](https://github.com/couchapp/couchapp) scripts make it easy to create and deploy these applications.

This project is aimed at creating some additional scripts that work along side couchapp to make application development process even easier.

This is really just a sandbox for testing out some ideas. Hopefully, any scripts that prove really usefull can be integrated back into CouchApp itself.

### A proxy server for client side development

For purely client side application development, its tedious to constantly run:
    $ couchapp push

In this case it would be more convenient to simple serve all the static files from the local filesystem and proxy the dymanic URLs to the CouchDB test server.

To achieve this a configuration script needs to be written to map between the applications URL space and the source files.

    $ couchapp-run [config.json] [port]

Now the application can be accessed via the proxy port (defaults to 8000) and changes to the static files will automatically show up with just a page refresh.

After some relaxing client side coding is complete

    $ couchapp push

will push all the changes to CouchDB server and the app can be accessed without the proxy.

### Server side couchapp developement

For now the normal couchapp push utility will be needed to update the design document itself (which is required for modifying the server side of a couchapp).

### License

couchapp-utils is licensed the same as CouchApp which is the [Apache 2.0 License](http://www.apache.org/licenses/LICENSE-2.0)

