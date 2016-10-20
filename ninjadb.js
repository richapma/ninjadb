/*
project: ninjadb
filename: ninjadb
author: richard john chapman
date: 2016-Sep-14
description: node db server.
comments:
//structure of id: {quarter:0-3=>1-4}/{month:0-2=>1-3}/{week:0-3=>1-4}/{day:0-6=>1-7}/{hh:0-9ab=>1-12}/{min:0-9=>00-50}/{seconds:0-6=>00-50/{milliseconds:0-9=>00-90/milliseconds_countid_tablenode.rec}

TO DO:
make fields prefixed with _ to be automatically indexed.
if the object being saved has fields prefixed with an __ it is automatically indexed for text searching.
prefixes might be changed...
maybe add stat files to different file structure nodes and update them as records are inserted so always up to date - with a rebuild feature.
maybe create a batch id, so all records updated/created are then associated with this batch.
add id index, see id.idx for format.
build function for handling where to store file etc. it is this function then that can be used to change how data is stored.
*/

//var fs = require('fs');
//https://github.com/isaacs/node-graceful-fs

var cluster = require('cluster')

process.stdin.resume(); //stop program closing instantly;

var merge_jsonstr = function(s1, s2){
    //make copy of objects pushed in.
    var o1 = JSON.parse(s1);
    var o2 = JSON.parse(s2);
    var o3;

    for (var attrname in o1) { o3[attrname] = o1[attrname]; }
    for (var attrname in o2) { o3[attrname] = o2[attrname]; }

    return JSON.stringify(o3);
}

if (cluster.isMaster) {
    // count the proc cores on machine.
    var cores = require('os').cpus().length;
    var worker = {};

    cores = 2;
    // make worker processes one for each core.
    for (var i = 0; i < cores; i += 1) {
        var fk = cluster.fork();
        
        console.log('new fork:' + fk);
        worker[fk.id] = fk;
      
    }
}else{
    
    var fs = require('graceful-fs');
    var events = require('events');
    var https = require('https');
    var http = require('http');
    var server_options = {
        key:    fs.readFileSync('key.pem'),
        cert:   fs.readFileSync('cert.pem')
    };
    
    var express = require('express');
    var io = require('socket.io');
    var io_client = require('socket.io-client');
    var cookieParser = require('cookie-parser');
    var bodyParser = require('body-parser');
    var expressSession = require('express-session'); //used but not with the memorystore.
    var in_bound;                //stores what is connected to this node.
    var out_bound = [];          //stores connections from this node to all other nodes on a per cpu basis.
    var web_server = null;       //server for web client access.
    var socket_server = null;    //server for web client access.
    var ninja = null;
    // create instance of express
    var app = express();
    var socket_app = express();

    var router = express.Router();
    // define middleware
    //ninjadb.use(express.static(path.join(__dirname, '../client')));
    //app.use(logger('dev'));
    app.use(bodyParser.json());
    app.use(bodyParser.urlencoded({ extended: false }));

    app.use(cookieParser());
    app.use(require('express-session')({
        secret: 'Ghp$^2S07^65@1#21lpA',
        resave: false,
        saveUninitialized: false
    }));

    socket_app.get('/', function(req, res){
        res.send('<h1>Hello world</h1>');
    });

    //global variables.
    var arg_obj = {}; //command line arguments object
    //var struct_cache = {};
    var open_files = {};
    var table_list = {};
    var node_list = {};

    var idcount = 0;  //initialize to zero.
    var months = [0, 0, 0, 1, 1, 1, 2, 2, 2, 3, 3, 3];  //return the quarter that the month is in.
    var weeks = [0, 0, 0, 0, 0, 0, 0, 0, 1, 1, 1, 1, 1, 1, 1, 1, 2, 2, 2, 2, 2, 2, 2, 2, 3, 3, 3, 3, 3, 3, 3]; //return the week the day is in.
    var hours = ['0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'a', 'b'];
    var mins = [0, 0, 0, 0, 0, 0,
                1, 1, 1, 1, 1, 1,
                2, 2, 2, 2, 2, 2,
                3, 3, 3, 3, 3, 3,
                4, 4, 4, 4, 4, 4,
                5, 5, 5, 5, 5, 5,
                6, 6, 6, 6, 6, 6,
                7, 7, 7, 7, 7, 7,
                8, 8, 8, 8, 8, 8,
                9, 9, 9, 9, 9];

    var millis = [0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                1, 1, 1, 1, 1, 1, 1, 1, 1, 1,
                2, 2, 2, 2, 2, 2, 2, 2, 2, 2,
                3, 3, 3, 3, 3, 3, 3, 3, 3, 3,
                4, 4, 4, 4, 4, 4, 4, 4, 4, 4,
                5, 5, 5, 5, 5, 5, 5, 5, 5, 5,
                6, 6, 6, 6, 6, 6, 6, 6, 6, 6,
                7, 7, 7, 7, 7, 7, 7, 7, 7, 7,
                8, 8, 8, 8, 8, 8, 8, 8, 8, 8,
                9, 9, 9, 9, 9, 9, 9, 9, 9, 9];

    var struct_cache_file = 'struct.cache';
    var table_list_file   = 'table.list';
    var node_list_file    = 'node.list';
    var access_list_file  = 'access.list';
    
    var load_time_slot_size = 2000; //time slot in milliseconds.
    var deltat;

    //app.use(express.static(path.join(__dirname, 'public')));
    router.put('/pass/:secret', function(req, res){
        //store the secret in cache for checking permissions.
        //***TODO:maybe implement. - depends if i decide on if the webserver side can be trusted to honor calling the request_connection first...
    });

    router.get('/pass/:secret', function(req, res){
        //receive secret.
        //***TODO:maybe implement. - depends if i decide on if the webserver side can be trusted to honor calling the request_connection first...
    });

    console.log('request connection');

    router.get('/request_connection/*', function(req, res){
        if(ninja.allow_access(req.ip)){
            var suggest_id = req.originalUrl.substring(20);
            
            res.status(200);
            //use load balencing to get the next node connection to use.
            var i = ninja.get_next_node(suggest_id);
            var options = {};
            
            //var secret = ninja.generate_id(new Date(), ninja.arg_obj.node, 0);
            
            if(ninja.node_list[i].ip6 != '')
            {
                options.host = ninja.node_list[i].ip6;
                options.port = ninja.node_list[i].port;
            }else{
                options.host = ninja.node_list[i].ip4;
                options.port = ninja.node_list[i].port;            
            }
            
            options.header = {accept: '*/*',
                              'Cache-Control': 'no-cache',
                              'data-secret': secret};
            //return the connection info to use.
            res.send(options);
        }else{
            //only the master is allowed to issue connections.
            res.status(403);
        }
    });

    console.log('new id');
    //routes
    router.get('/new_id/:table_node', function(req, res){
        if(ninja.allow_access(req.ip)){
            var id = ninja.generate_id(new Date(), self.arg_obj.node, req.params.table_node);
            res.status(200);
            res.send(id);
        }else{
            res.status(403);
        }
    });

    console.log('read');
    //needs to be updated to go to the correct location.
    router.get('/read/*', function(req, res){
        //req.params.search = req.params.search.replace(/~/g,'.*');
        //var rstream = fs.createReadStream('existFile');
        //rstream.pipe(res);
        console.log('read req from:' + req.ip);
        if(ninja.allow_access(req.ip)){
            var id = req.originalUrl.substring(6);
            console.log(req.originalUrl);
            console.log('path:' + ninja.arg_obj.root + '/' + id + '.rec');
            //res.setHeader("content-type", "some/type");
            var rs = fs.createReadStream(ninja.arg_obj.root + '/' + id + '.rec');


            rs.on('error', function(e){
                try{
                    rs.end(); //just incase rs got left open.
                }catch(e2){
                    //we tried ;-)
                }

                console.log(e);
                console.log('error: failed to create read stream to use.')
            });

            console.log('read id:' + req.originalUrl.substring(10)); 
            rs.pipe(res);
        }else{
            console.log('access denied');
            res.status(403);
        }
    });

    console.log('write');
    router.put('/write/*', function(req, res){
        if(ninja.allow_access(req.ip)){
            var id = req.originalUrl.substring(7);

            if(id == ''){
                //no id provided - generate one.
            }else{
                //id + '.rec'
            }

            console.log(req.originalUrl.substring(7)); 
            res.send("write called.");
            
            res.status(200);
        }else{
            res.status(403);
        }
    });

    console.log('app use');
    app.use('/', router);

    //app_socket.use(function(req,res){});
    // error
    app.use(function(req, res, next) {
    var err = new Error('Not Found');
    err.status = 404;
    next(err);
    });

    app.use(function(err, req, res) {
    res.status(err.status || 500);
    res.end(JSON.stringify({
        message: err.message,
        error: {}
    }));
    });

    function help(mess) {
        console.log('Help:');
        console.log('--root "path to root database folder"');
        console.log('--node_list "full path to a file containing the list of all nodes in the cluster"');
        console.log('--node {identify the node position i am in the list_nodes list}');
        console.log('--reset_count {how often to reset the id counter measured in seconds}');
    }

    function check_args(arg_o) {
        //***r add in check the arg_obj for valid/invalid parameters.
    }

    //***r this probably needs rewrite.
    function writefs_struct_cache(path, cache) {
        /*fs.writeFile(arg_obj.root + '/' + struct_cache_file, JSON.stringify(struct_cache) , 'utf8', (err) => {
            if (err) throw err;        
        });*/

        var ws = fs.createWriteStream(arg_obj.root + '/' + arg_obj.node + '/' + struct_cache_file);
        ws.setEncoding = 'utf8';
        ws.write(JSON.stringify(glob.struct_cache));
        ws.end();
    }

    function writefs_table_list(path, tables) {
        var ws = fs.createWriteStream(arg_obj.root + '/' + arg_obj.node + '/' + table_list_file);
        ws.setEncoding = 'utf8';
        ws.write(JSON.stringify(tables));
        ws.end();
    }

    var ninjadb = function() { 
        var self = this;
        var comline_args = process.argv.slice(2);

        self.init_count = 0;

        //check the number of parameters are even.
        if (comline_args.length % 2 != 0) {
            console.log('Error: Invalid parameter count, script aborted!');
            help();
        }

        self.arg_obj = self.get_arg_obj(comline_args);
        console.log(self.arg_obj);
    }

    ninjadb.prototype = new events.EventEmitter;

    ninjadb.prototype.emit_to_cpus = function(obj)
    {
        
        for(var j=0; j<self.node_list[self.arg_obj.node].cpu_count; j++){
            out_bound[self.arg_obj.node][j]
        }
    }

    ninjadb.prototype.init_complete = function(){
        var self = this;
        console.log('init complete');
        console.log('init count:' + self.init_count);
        if(self.init_count > 3)
        {
            //inits complete. start the server.
            //ninjadb.get('port', process.env.PORT || 3000);
            //***TODO: find the node that belongs to us. 
            console.log('starting server...');
            web_server = https.createServer(server_options, app).listen(parseInt(self.node_list[self.arg_obj.node].port), '::', function(){
                    console.log('Ninjadb webserver listening on port ' + web_server.address().port);
                });

            //server_options, 
            socket_server = https.createServer(server_options, socket_app).listen((parseInt(self.node_list[self.arg_obj.node].wss_from_port) + parseInt(cluster.worker.id-1)), '::', function(){
                    console.log('Ninjadb socket listening on port ' + socket_server.address().port);
                });

            in_bound = io.listen(socket_server);
            in_bound.on('connection', function(socket) { 
                console.log('socket connection on server.');

                in_bound.emit('echo','hello');
                //wire up events.
                in_bound.on('echo', function(data){
                    console.log('received echo event on server');
                });

                in_bound.on('update_cache', function (data){
                    //update the local cache.
                    glob.struct_cache[data.key] = 1;
                }); 
            });

            in_bound.on('error', function(obj){console.log(JSON.stringify(obj));});

            //attempt to establish connections to all other nodes now.
            console.log('i am node:' + self.arg_obj.node + ' cpu:' + (parseInt(cluster.worker.id)-1));
            var counting = -1;
            var worker_id = null;

            for(var i in self.node_list) {
                if(self.node_list.hasOwnProperty(i)){
                    var start_port = parseInt(self.node_list[i].wss_from_port);
                    out_bound[i] = [];
                    for(var j=0; j<self.node_list[i].cpu_count; j++){
                        counting++;
                        worker_id = parseInt(cluster.worker.id)-1;
                        //do not connect to self.
                        if(i != self.arg_obj.node || (i == self.arg_obj.node && j != worker_id)){
                            console.log('node:' + i + ' cpu:' + j);
                            console.log('port:' + (parseInt(self.node_list[i].wss_from_port) + j));
                            if(self.node_list[i].ip6){
                                console.log('ip6:' + self.node_list[i].ip6 + ':' + (start_port + j));
                                out_bound[i][j] = io_client.connect('http://[' + self.node_list[i].ip6 + ']:' + (start_port + j), {
                                    'reconnection': true,
                                    'reconnectionDelay': 1000
                                });
                            }else{
                                console.log('attempting to connect to ip4:' + self.node_list[i].ip4 + ':' + (start_port + j));
                                out_bound[i][j] = io_client.connect('https://' + self.node_list[i].ip4 + ':' + (start_port + j), {
                                    'secure': true,
                                    'transports': ['websocket'],
                                    'reconnection': true,
                                    'reconnectionDelay': 1000
                                });
                            }
                        
                            out_bound[i][j].on('connect', function (socket){
                                console.log('connected!');
                            });

                            out_bound[i][j].on('connect_error', function(err){console.log('connect error:'+JSON.stringify(err));});

                            out_bound[i][j].on('reconnecting', function(err){console.log('reconnecting:'+JSON.stringify(err));});

                            out_bound[i][j].on('reconnect_error', function(err){console.log('reconnect error:'+JSON.stringify(err));});

                            out_bound[i][j].on('reconnect_failed', function(err){console.log('reconnect failed:'+JSON.stringify(err));});

                            out_bound[i][j].on('echo', function (data) {
                                var worker_id = parseInt(cluster.worker.id)-1;
                                console.log('echo received on node:' + self.arg_obj.node + ' cpu:' + worker_id);
                            });

                        }
                    }
                }
            }
        }
    }
    

    ninjadb.prototype.init_node_list = function(){
        var self = this;

        var rs = fs.createReadStream(self.arg_obj.root + '/' + node_list_file);
        var data = [];

        rs.setEncoding('utf8');

        rs.on('error', function(e){
            self.node_list = {};
        });

        rs.on('data', function(chunk) {
            data.push(chunk);
        });

        rs.on('end', function() {
            self.node_list = JSON.parse(data.join());
            self.node_list_length = 0;
            for(var i in self.node_list) {
              if(self.node_list.hasOwnProperty(i)){
                self.node_list_length++;
              }
            }

            deltat = load_time_slot_size / self.node_list_length;

            self.init_count++;
            self.init_complete();
            self.init_access_list();
        });
    }

    ninjadb.prototype.init_access_list = function(){
        var self = this;
        var n;

        console.log('init_access_list');
        self.access_list = {};
        
        console.log(self.node_list);
        for(var i in self.node_list) {
            if(self.node_list.hasOwnProperty(i)){
                self.access_list[self.node_list[i].ip4] = 1;
                self.access_list[self.node_list[i].ip6] = 1; 
            }
        }
        
        console.log(self.access_list);
        self.init_count++;
        self.init_complete();

    }

    ninjadb.prototype.init_cache = function(){
        var self = this;

        console.log('init_cache');

        var rs = fs.createReadStream(self.arg_obj.root + '/' + self.arg_obj.node + '/' + struct_cache_file);
        var data = [];

        rs.setEncoding('utf8');

        rs.on('error', function(e){
            console.log('error could not read cache');
            self.init_count++; //ignore failure.
            self.init_complete();
        })

        rs.on('data', function(chunk) {
            data.push(chunk);
        });

        rs.on('end', function() {
            console.log('init cache:' + glob.struct_cache);
            //glob.struct_cache = JSON.parse(data.join());
            console.log('init cache:' + glob.struct_cache);
            self.init_count++;
            self.init_complete();
        });
    }

    ninjadb.prototype.init_table_list = function(){
        var self = this;
        console.log('init_table_list');
        var rs = fs.createReadStream(self.arg_obj.root + '/' + table_list_file);
        var data = [];

        rs.setEncoding('utf8');

        rs.on('error', function(e){
            self.table_list = [];
        })

        rs.on('data', function(chunk) {
            data.push(chunk);
        });

        rs.on('end', function() {
            self.table_list = JSON.parse(data.join());
            self.init_count++;
            self.init_complete();
        });
    }

    ninjadb.prototype.get_arg_obj = function(arr) {
        var self = this;
        var arg = {};
        for (var i = 0; i < arr.length; i += 2) {
            arg[arr[i].substring(2)] = arr[i + 1];
        }

        arg.node = parseInt(arg.node);
        return arg;
    }

    ninjadb.prototype.oid_to_path = function(id_obj) {
        var self = this;
        //{quarter:0-3=>1-4}/{month:0-2=>1-3}/{week:0-3=>1-4}/{day:0-6=>1-7}/{hh:0-9ab=>1-12}/{min:0-9=>00-50}/{seconds:0-6=>00-50/{milliseconds:0-9=>00-90/milliseconds-countid-table_node.rec}
        //the remainder of the id is dynamic and the folder structure will be built as id is generated.
        console.log(id_obj);
        return id_obj.s1.join('/') + '/' + id_obj.s2.join('_');
    }

    ninjadb.prototype.recursive_create_dir = function(id_obj, depth, max, callback) {
        var self = this;
        //console.log('recursive_create_dir:'+id_obj);
        if (depth > max) {
            //we have gone through all the id entries. we are done.
            //console.log('recusive_dir_complete:');
            return callback();
        }
        var new_depth = depth+1;
        var key = id_obj.s1.slice(0, new_depth).join('/');
        console.log('recursive:' + JSON.stringify(glob));
        //glob.struct_cache[key] = 1;
        if (glob.struct_cache[key] > 0) {
            //folder already exists we are done.
            self.recursive_create_dir(id_obj, new_depth, max, callback);
        } else {
            var path = self.arg_obj.root + '/' + id_obj.s1.slice(0, new_depth).join('/');
            console.log('calling mkdir:' + path);
            console.log('recursive:' + JSON.stringify(glob));
            fs.mkdir(path,
                function(e) {
                    console.log('mk call back called');
                    console.log('recursive:' + JSON.stringify(glob));
                    if (!e) {
                        //created file
                        console.log('recursive key:' + key);
                        glob.struct_cache[key] = 1;
                        console.log('recursive:' + JSON.stringify(glob));
                        //process.send(JSON.stringify({src:cluster.worker.id, type:2, mess:key}));
                    } else {
                        console.log('recursive:' + JSON.stringify(glob));
                        if(e.code === 'EEXIST'){
                            glob.struct_cache[key] = 1;
                            //process.send(JSON.stringify({src:cluster.worker.id, type:2, mess:key}));
                        }
                    }
                    console.log('recursive:' + JSON.stringify(glob));
                    self.recursive_create_dir(id_obj, new_depth, max, callback);
                }
            );
        }
        
    }

    ninjadb.prototype.allow_access = function(ip){
        //check incoming request is from an address in the node.list
        var self = this;
        console.log(self.access_list);
        if(self.access_list[ip] > 0){
            return true;
        }else{
            return false;
        }
    }

    //dt = new Date();
    ninjadb.prototype.generate_id = function(dt, node, table_node) {
        var self = this;
        var mo = dt.getMonth(); //month in year (0-11)
        var dy = dt.getDay();    //day of week (0-6)
        var dm = dt.getDate() - 1; //day of month (1-31)
        var hr = dt.getHours();  //hour of day
        var min = dt.getMinutes(); //mins in hour (0-59)
        var sec = dt.getSeconds(); //seconds in min (0-59)
        var ms = dt.getMilliseconds(); //milliseconds in second (000-999).
        var qu = months[mo]; //quarter
        var wk = weeks[dm];
    //    if (hr > 12) {
    //        hr = hr - 12;
    //    }
        hr = hours[hr % 12];
        mo = hours[mo % 12];
        min = mins[min];
        mst = Math.trunc(ms/10.0); //(00.0-99.9)=>(00-99)
        console.log(mst);

        var id_obj = {
            s1: [node, qu, mo, wk, dy, hr, mins[min], mins[sec], millis[mst]],
            s2: [ms, idcount++, table_node]
        };

        
        var path = self.oid_to_path(id_obj);
        console.log(id_obj);
        self.recursive_create_dir(id_obj, 0, id_obj.s1.length, function(){
            //create file for testing.
            //self.create_file_async(self.arg_obj.root + '/' + path, false);
        });

        console.log(path);
        
        return path;
    }

    ninjadb.prototype.init = function() {
        var self = this;
        self.init_count=0;

        self.init_node_list();
        self.init_table_list();
        self.init_cache();

        //process.env.src = cluster.worker.id;
        glob = {};

        console.log(glob);
        console.log('init:' + JSON.stringify(glob));
    };

    ninjadb.prototype.get_milliseconds = function(){
        var self=this;
        var dt = new Date();

        var mill = (dt.getSeconds()*1000+dt.getMilliseconds) % load_time_slot_size;
        return mill;
    }

    ninjadb.prototype.get_next_node = function(suggest_id){
        var self=this;

        console.log(glob);
        if(suggest_id != ''){
            //***TODO: check the suggestion is even valid.
            return suggest_id;
        }else{
            //use load balancing algorithm to choose database store to use.
            //slice a time window with number of nodes
            return math.floor(this.get_milliseconds()/deltat);
        }
    }
    
    ninjadb.prototype.writefs_struct_cache_sync = function(path, cache) {
        fs.writeFileSync(path, JSON.stringify(cache) , 'utf8', (err) => {
            if (err) throw err;        
        });
    }

    ninjadb.prototype.exitHandler = function(options, err) {
        //NO ASYNC FUNCTIONS ALLOWED HERE!.
        //attempt to save the struct_cache.
        var self=this;
        console.log('exit handler:' + err);
        //writefs_struct_cache_sync(nj.arg_obj.root + '/' + nj.arg_obj.node + '/' + struct_cache_file + '~dump', JSON.stringify(nj.struct_cache));
        process.exit();
    }

    ninjadb.prototype.

    ninjadb.prototype.test = function(){

    }

    ninja = new ninjadb();

    //do something when app is closing
    process.on('exit', ninja.exitHandler.bind(null,{exit:true}));

    //catches ctrl+c event
    process.on('SIGINT', ninja.exitHandler.bind(null, {exit:true}));

    //catches uncaught exceptions
    process.on('uncaughtException', ninja.exitHandler.bind(null, {exit:true}));

    ninja.init();

}

cluster.on('exit', function (worker) {
    //worker/forked proc died, restart another.
    console.log('Worker %d died', worker.id);
    cluster.fork();
});

