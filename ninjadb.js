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
var fs = require('fs');
//https://github.com/isaacs/node-graceful-fs
//var fs = require('graceful-fs');

process.stdin.resume(); //stop program closing instantly;

function writefs_struct_cache_sync(path, cache) {
    fs.writeFileSync(path, JSON.stringify(cache) , 'utf8', (err) => {
        if (err) throw err;        
    });
}

function exitHandler(options, err) {
    //NO ASYNC FUNCTIONS ALLOWED HERE!.
    //attempt to save the struct_cache.
    console.log(err);
    writefs_struct_cache_sync(arg_obj.root + '/' + arg_obj.node + '/' + struct_cache_file + '~dump', JSON.stringify(struct_cache));
    process.exit();
}

//do something when app is closing
process.on('exit', exitHandler.bind(null,{exit:true}));

//catches ctrl+c event
process.on('SIGINT', exitHandler.bind(null, {exit:true}));

//catches uncaught exceptions
process.on('uncaughtException', exitHandler.bind(null, {exit:true}));

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
var table_list_file =   'table.list';

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

function oid_to_path(id_obj) {
    //{quarter:0-3=>1-4}/{month:0-2=>1-3}/{week:0-3=>1-4}/{day:0-6=>1-7}/{hh:0-9ab=>1-12}/{min:0-9=>00-50}/{seconds:0-6=>00-50/{milliseconds:0-9=>00-90/milliseconds-countid-table_node.rec}
    //the remainder of the id is dynamic and the folder structure will be built as id is generated.
    return id_obj.s1.join('/') + '/' + id_obj.s2.join('_') + '.rec';
}

//create the file to store the data for the given id and keep a handle open to it as a write is probably be soon.
//***r probably need to create something to clean up closing these if they are deemed open too long!
function create_file_async(path, keepopen){
    console.log(path);
    fs.open(path, 'a+', function (e, fd) {
        console.log(e);
        console.log('file desc:' + fd)
        if(!e){
            open_files[path] = {fdesc: fd, dt: new Date()};
        }
        if(!keepopen){
            fs.close(fd, function (e) { });
        }
    });
}

function recursive_create_dir(id_obj, depth, max, callback) {
    if (depth > max) {
        //we have gone through all the id entries. we are done.

        console.log('recusive_dir_complete:');
        return callback();
    }
    var new_depth = depth+1;
    var key=id_obj.s1.slice(0, new_depth).join();

    if (struct_cache[key] > 0) {
        //folder already exists we are done.
        console.log('cache exists:');
        recursive_create_dir(id_obj, new_depth, max, callback);
    } else {
        var path = arg_obj.root + '/' + id_obj.s1.slice(0, new_depth).join('/');
        console.log('calling mkdir:' + path);
        fs.mkdir(path,
            function(e) {
                console.log('mk call back called');
                if (!e) {
                    //hmm
                    struct_cache[key] = 1;
                } else {
                    if(e.code === 'EEXIST'){
                        struct_cache[key] = 1;
                    }else{
                        if(struct_cache[key] > 0){
                            //cache is invalidated.
                            struct_cache[key] = 0;
                            //***r: maybe should invalidate the chain for all folders below this too?
                        }
                        //hmmm maybe the cache was wrong?
                        log.console('failed to create directory: ' + path);
                    }
                }
                recursive_create_dir(id_obj, new_depth, max, callback);
            }
        );
        
    }
}

//dt = new Date();
function generate_id(dt, count_id, table_node) {
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
        s1: [arg_obj.node, qu, mo, wk, dy, hr, mins[min], mins[sec], millis[mst]],
        s2: [ms, idcount++, table_node]
    };

    console.log(id_obj);
    var path = oid_to_path(id_obj);

    recursive_create_dir(id_obj, 0, id_obj.s1.length, function(){
        create_file_async(arg_obj.root + '/' + path, false);
    });

    
    console.log(path);
     //***r this needs to be changed to true;
    return path;
}

function check_args(arg_o) {
    //***r add in check the arg_obj for valid/invalid parameters.
}

function get_arg_obj(arr) {
    var arg = {};
    for (var i = 0; i < arr.length; i += 2) {
        arg[arr[i].substring(2)] = arr[i + 1];
    }
    return arg;
}

//***r this probably needs rewrite.
function readfs_struct_cache(path) {
    console.log('attempting to open stream');
    var rs = fs.createReadStream(arg_obj.root + '/' + arg_obj.node + '/' + struct_cache_file);
    var data = [];

    rs.setEncoding('utf8');

    rs.on('error', function(e){console.log(e)})

    rs.on('data', function(chunk) {
        data.push(chunk);
    });

    rs.on('end', function() {
        struct_cache = JSON.parse(data.join());
    });
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

function readfs_table_list(path) {
}

function writefs_table_list(path, tables) {
}

//check if table exists.


function init() {
    //process the command line parameters with format --a valueofa --b valueofb --c valueofc
    var comline_args = process.argv.slice(2);

    //check the number of parameters are even.
    if (comline_args.length % 2 != 0) {
        console.log('Error: Invalid parameter count, script aborted!');
        help();
    }
    
    arg_obj = get_arg_obj(comline_args);
    console.log(arg_obj);

    //***r read struct_cache from file.
    //readfs_struct_cache(struct_cache_file);
    //console.log(struct_cache);

    //***r read the list of tables from file that exist in db (this composes the last part of the id)
    //table_list = readfs_table_list(path);

    //read the lnodes file into memory.
    //node_list = readfs_node_list(path);

    //check which nodes are available locally and mark it in the in memory lnodes file.
    //{root folder}/{position in lnodes file}/

    //check static file structure is built (part of first 6 digits of internal id).
    //conceptional mapping.
    //{quarter:0-3=>1-4}/{month:0-2=>1-3}/{week:0-3=>1-4}/{day:0-6=>1-7}/{hh:0-9ab=>1-12}/{min:0-9=>00-50}/{miliseconds:0-6=>00000-60000}/{miliseconds:0-9=>0000-9000/miliseconds-countid.json}
    //the remainder of the id is dynamic and the folder structure will be built as id is generated.

    
        setTimeout(function(){
        var start_date = new Date();
        console.log('started:' + start_date.toISOString());
        for(var i=0; i<100; i++){
            generate_id(new Date(), idcount, 0);
        }
            var end_date = new Date();

            var diff = (end_date - start_date);
            console.log('completed:' + end_date.toISOString());
            console.log('time elapsed:' + diff + 'ms');
        }, 5000);



    writefs_struct_cache(struct_cache_file);
}

init();