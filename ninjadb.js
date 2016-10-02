/*
project: ninjadb
filename: ninjadb
author: richard john chapman
date: 2016-Sep-14
description: node db server.
comments: if the object being saved has fields prefixed with an _ its value is automatically indexed on it treating it as an internal id type that ninjadb uses.
if the object being saved has fields prefixed with an __ it is automatically indexed for text searching.
prefixes might be changed...
maybe add stat files to different file structure nodes and update them as records are inserted so always up to date - with a rebuild feature.
*/

//***TO DO:
//track stats on node.list for use in load balancing
//update read function to forward to the node that has the data
//add function to request node to use from nodes.
//add security check based on incomming ip address has to be one of the nodes.
//create a batch id, so all records updated/created are then associated with this batch.

//var fs = require('fs');
//https://github.com/isaacs/node-graceful-fs

/*
TODO: Work adding messaging code so main thread can broadcast to sub threads so each worker can be updated with the current statistics of what secrets and node connections have been handed out.
so load balencing can use the correct info.


From Master to worker:
worker.send({json data});    // In Master part

process.on('message', yourCallbackFunc(jsonData));    // In Worker part

From Worker to Master:
process.send({json data});   // In Worker part

worker.on('message', yourCallbackFunc(jsonData));    // In Master part

*/

var cluster = require('cluster')

process.stdin.resume(); //stop program closing instantly;

var master_load_bal_stats = {
    chosen_node : 0
};

process.on('message', function(mess){
    //received a message from one of the workers.
    //broadcast message to all workers.
    for(var thisworker in worker){
        thisworker.send(mess);
    }
});

if (cluster.isMaster) {
    // count the proc cores on machine.
    var cores = require('os').cpus().length;
    var worker = [];
    process.env.load_bal_stats = JSON.stringify(master_load_bal_stats);

    // make worker processes one for each core.
    for (var i = 0; i < cores; i += 1) {
        var fk = cluster.fork({load_bal_stats: JSON.stringify(master_load_bal_stats)});
        
        console.log(process.env);
        worker[fk.id] = fk;

        worker[fk.id].on('message', function(mess){
            var self = this;
            process.env.load_bal_stats = mess;
            console.log(process.env);
        });
    }
}else{
    var fs = require('graceful-fs');
    var events = require('events');
    var https = require('https');
    var server_options = {
        key:    fs.readFileSync('key.pem'),
        cert:   fs.readFileSync('cert.pem')
    };
    
    var express = require('express');
    var cookieParser = require('cookie-parser');
    var bodyParser = require('body-parser');
    var expressSession = require('express-session'); //used but not with the memorystore.
    var server = null;
    var ninja = null;
    // create instance of express
    var app = express();
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

    //global variables.
    var arg_obj = {}; //command line arguments object
    var struct_cache = {};
    var open_files = {};
    var table_list = [];
    var node_list = [];

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
    //var load_bal_stats = {};

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
    //check i am the master database node and if i am return the node i want the consumer to use.
    router.get('/request_connection/*', function(req, res){
        if(ninja.allow_access(req.ip) && ninja.node_list[ninja.arg_obj.node].type == 'master'){
            var suggest_id = req.originalUrl.substring(20);
            console.log(suggest_id);
            //i am the master check 
            res.status(200);
            //use load balencing to get the next node connection to use.
            var i = ninja.get_next_node(suggest_id);
            var options = {};
            
            var secret = ninja.generate_id(new Date(), ninja.arg_obj.node, 0);
            //***TODO: need to pass secret to database that will be accessed.
            
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
        /*file format of --node_list is:
        [
        {dns:'localhost', ip4:'127.0.0.1', ip6:'', port:2000, syncnodes:[0,1], synctype: 0},
        {dns:'localhost', ip4:'127.0.0.1', ip6:'', port:2000, syncnodes:[0,1], synctype: 0},
        {dns:'localhost', ip4:'127.0.0.1', ip6:'', port:2000, syncnodes:[0,1], synctype: 1}
        ]
        */
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
        ws.write(JSON.stringify(struct_cache));
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
            server = https.createServer(server_options, app).listen(self.node_list[self.arg_obj.node].port, '::', function(){
                    console.log('Ninjadb listening on port ' + server.address().port);
                });
            /*server = app.listen(self.node_list[self.arg_obj.node].port, '::', function() {
                console.log('Ninjadb listening on port ' + server.address().port);
            });*/
        }
    }

    ninjadb.prototype.init_node_list = function(){
        var self = this;

        var rs = fs.createReadStream(self.arg_obj.root + '/' + node_list_file);
        var data = [];

        rs.setEncoding('utf8');

        rs.on('error', function(e){
            self.node_list = [];
        });

        rs.on('data', function(chunk) {
            data.push(chunk);
        });

        rs.on('end', function() {
            self.node_list = JSON.parse(data.join());
            console.log('init_node_list:' +  self.node_list.length);
            self.node_list_length = self.node_list.length;
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
        for (var i=0; i<self.node_list_length; i++){
            self.access_list[self.node_list[i].ip4] = 1;
            self.access_list[self.node_list[i].ip6] = 1;                    
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
            self.struct_cache = {};
            self.init_count++; //ignore failure.
            self.init_complete();
        })

        rs.on('data', function(chunk) {
            data.push(chunk);
        });

        rs.on('end', function() {
            self.struct_cache = JSON.parse(data.join());
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
        return arg;
    }

    ninjadb.prototype.oid_to_path = function(id_obj) {
        var self = this;
        //{quarter:0-3=>1-4}/{month:0-2=>1-3}/{week:0-3=>1-4}/{day:0-6=>1-7}/{hh:0-9ab=>1-12}/{min:0-9=>00-50}/{seconds:0-6=>00-50/{milliseconds:0-9=>00-90/milliseconds-countid-table_node.rec}
        //the remainder of the id is dynamic and the folder structure will be built as id is generated.
        console.log(id_obj);
        return id_obj.s1.join('/') + '/' + id_obj.s2.join('_') + '.rec';
    }

    ninjadb.prototype.recursive_create_dir = function(id_obj, depth, max, callback) {
        var self = this;
        console.log('recursive_create_dir:'+id_obj);
        if (depth > max) {
            //we have gone through all the id entries. we are done.
            console.log('recusive_dir_complete:');
            return callback();
        }
        var new_depth = depth+1;
        var key=id_obj.s1.slice(0, new_depth).join();

        if (self.struct_cache[key] > 0) {
            //folder already exists we are done.
            console.log('cache exists:');
            self.recursive_create_dir(id_obj, new_depth, max, callback);
        } else {
            var path = self.arg_obj.root + '/' + id_obj.s1.slice(0, new_depth).join('/');
            console.log('calling mkdir:' + path);
            fs.mkdir(path,
                function(e) {
                    console.log('mk call back called');
                    if (!e) {
                        //hmm
                        self.struct_cache[key] = 1;
                    } else {
                        if(e.code === 'EEXIST'){
                            self.struct_cache[key] = 1;
                        }else{
                            if(self.struct_cache[key] > 0){
                                //tcache is invalidated.
                                self.struct_cache[key] = 0;
                                //***r: maybe should invalidate the chain for all folders below this too?
                            }
                            //hmmm maybe the cache was wrong?
                            log.console('failed to create directory: ' + path);
                        }
                    }
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

    };

    ninjadb.prototype.get_next_node = function(suggest_id){
        var self=this;
        var js_env_lbs = JSON.parse(process.env.load_bal_stats);

        if(suggest_id != ''){
            //***TODO: check the suggestion is even valid.
            //update load_bal_stats.
            //this should be a suggestion - not a demand... balancing algorithm should have opertunity to decide over it.
            js_env_lbs.chosen_node = suggest_id;
            process.env.load_bal_stats = JSON.stringify(js_env_lbs);
        }else{
            //use load balancing algorithm to choose database store to use.
            console.log('get next node, node_list_length:' + self.node_list_length);
            console.log('the node list' + self.node_list);
            //currently load balance is just round robin.
            do{
                console.log('current node:' + js_env_lbs.chosen_node);
                js_env_lbs.chosen_node = (js_env_lbs.chosen_node + 1) % (self.node_list_length);
                console.log('get next node:' + js_env_lbs.chosen_node);
            }while(self.node_list[js_env_lbs.chosen_node].type != 'node')
            process.env.load_bal_stats = JSON.stringify(js_env_lbs);
        }
        process.send(process.env.load_bal_stats); //inform parent process to broadcast to all forks
        return js_env_lbs.chosen_node;
    }
    
    ninjadb.prototype.writefs_struct_cache_sync = function(path, cache) {
        var self = this;
        fs.writeFileSync(path, JSON.stringify(cache) , 'utf8', (err) => {
            if (err) throw err;        
        });
    }

    ninjadb.prototype.exitHandler = function(options, err) {
        var self = options;
        //NO ASYNC FUNCTIONS ALLOWED HERE!.
        //attempt to save the struct_cache.
        console.log(err);
        console.log(self);
        self.writefs_struct_cache_sync(self.arg_obj.root + '/' + self.arg_obj.node + '/' + struct_cache_file + '~dump', JSON.stringify(self.struct_cache));
        process.exit();
    }


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

    console.log(process.env);
}

cluster.on('exit', function (worker) {
    //worker/forked proc died, restart another.
    console.log('Worker %d died', worker.id);
    cluster.fork();
});

