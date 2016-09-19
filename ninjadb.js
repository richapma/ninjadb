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

/* along with syntax to indicate table etc and qualifying conditions. */

//var fs = require('fs');
//https://github.com/isaacs/node-graceful-fs
var fs = require('graceful-fs');
var events = require('events');
var express = require('express');
var cookieParser = require('cookie-parser');
var bodyParser = require('body-parser');
var expressSession = require('express-session'); //used but not with the memorystore.
var server = null;
var ninja = null;

process.stdin.resume(); //stop program closing instantly;

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
    saveUninitialized: false,
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

//app.use(express.static(path.join(__dirname, 'public')));

//routes
router.get('/new_id/:table_node', function(req, res){
    //test to see if 
    var id = ninja.generate_id(new Date(), req.params.table_name);

    res.send(id);
});

router.get('/read/*', function(req, res){
    //req.params.search = req.params.search.replace(/~/g,'.*');
    //var rstream = fs.createReadStream('existFile');
    //rstream.pipe(res);

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
});

router.put('/write/*', function(req, res){
    var id = req.originalUrl.substring(7);

    if(id == ''){
        //no id provided - generate one.
    }

    console.log(req.originalUrl.substring(7)); 
    res.send("write called.");
});

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

    if(self.init_count > 2)
    {
        //inits complete. start the server.
        //ninjadb.get('port', process.env.PORT || 3000);
        server = app.listen(self.node_list[self.arg_obj.node].port, function() {
            console.log('Ninjadb listening on port ' + server.address().port);
        });
    }
}

ninjadb.prototype.init_node_list = function(){
    var self = this;

    console.log('init_node_list');
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
        self.init_count++;
        self.init_complete();
    });
}

ninjadb.prototype.table_exist = function(table){
    var self = this;

    if(self.table_list[table]){
        return true;
    }else{
        return false;
    }
}

ninjadb.prototype.init_cache = function(){
    var self = this;

    console.log('init_cache');

    var rs = fs.createReadStream(self.arg_obj.root + '/' + self.arg_obj.node + '/' + struct_cache_file);
    var data = [];

    rs.setEncoding('utf8');

    rs.on('error', function(e){
        self.struct_cache = {};
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
    return id_obj.s1.join('/') + '/' + id_obj.s2.join('_') + '.rec';
}

ninjadb.prototype.recursive_create_dir = function(id_obj, depth, max, callback) {
    var self = this;
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
        var path = self.arg_obj.root + '/' + self.id_obj.s1.slice(0, new_depth).join('/');
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
                            //cache is invalidated.
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

//dt = new Date();
ninjadb.prototype.generate_id = function(dt, table_node) {
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
        s1: [self.arg_obj.node, qu, mo, wk, dy, hr, mins[min], mins[sec], millis[mst]],
        s2: [ms, idcount++, table_node]
    };

    console.log(id_obj);
    var path = self.oid_to_path(id_obj);

    self.recursive_create_dir(id_obj, 0, id_obj.s1.length, function(){
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

ninjadb.prototype.writefs_struct_cache_sync = function(path, cache) {
    var self = this;
    fs.writeFileSync(path, JSON.stringify(cache) , 'utf8', (err) => {
        if (err) throw err;        
    });
}

ninjadb.prototype.exitHandler = function(options, err) {
    var self = this;
    //NO ASYNC FUNCTIONS ALLOWED HERE!.
    //attempt to save the struct_cache.
    console.log(err);
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

module.exports = app;
