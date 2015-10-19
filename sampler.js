var redis = require('redis');
var _ = require('lodash');
var async = require('async');

// Parse arguments.
var args = process.argv.slice(2);
var realArgs = [], argOptions = {};
_.each(args, function(arg) {
    if (arg.indexOf('--') === 0) {
        arg = arg.substr(2);
        var eqI = arg.indexOf('=');
        if (eqI >= 0) {
            argOptions[arg.substr(0, eqI)] = arg.substr(eqI + 1);
        } else {
            argOptions[arg] = null;
        }
    } else {
        realArgs.push(arg);
    }
});

if (!realArgs.length || !realArgs[0]) {
    console.error("Usage: node sampler REDIS_MATCH [REGULAR_EXPR] [--regex-inverse] [--delete=SECURITY_HASH] [--reset-expire=SECONDS] [--max-levels=LEVELS]");
}

if (argOptions.hasOwnProperty('delete')) {
    // Check if security hash is correct.
    var crypto = require('crypto');

    var str = realArgs.join(' ');
    var otherOptions = {};
    _.each(argOptions, function(value, key) {
        if (key != 'delete') {
            str += ' ' + key + (value ? '=' + value : '');
        }
    });

    var hash = crypto.createHash('md5').update(str).digest('hex');
    if (hash != argOptions['delete']) {
        console.error('Deleting is dangerous. Please check all arguments and specify the following delete security hash: ' + hash);
        process.exit(1);
    }
}

var redisClient = redis.createClient({host: argOptions['host'] || '127.0.0.1', port: argOptions['port'] || '6379'});

/**
 * Executes the command.
 */
var exec = function() {
    process.on('SIGINT', function () {
        outputGrouped();
        process.exit(2);
    });

    console.log('Getting all keys..');

    // Start fetching keys in the background.
    getAllKeys(function(err) {
        if (err) {
            console.error(err);
            process.exit(1);
        }

        console.log('All keys fetched.');
    });

    // Sample keys in parallel.
    startSampling(function(err) {
        if (err) {
            console.error(err);
            process.exit(1);
        }

        console.log('all keys done');
        outputGrouped();
        process.exit(0);
    });

};

var outputGrouped = function() {
    console.log('Grouping output..');

    var measurementsTree = {parent: null, count: 0, size: 0, children: {}};

    var getNode = function(node, groups, maxLevels) {
        var item = groups.shift();

        if (!node.children.hasOwnProperty(item)) {
            node.children[item] = {parent: node, count: 0, size: 0, children: {}};
        }

        if (groups.length == 0 || maxLevels === 0) {
            return node.children[item];
        } else {
            return getNode(node.children[item], groups, maxLevels === null ? null : maxLevels - 1);
        }
    };

    _.each(measurements, function(size, key) {
        var groups = key.split(':');

        var node = getNode(measurementsTree, groups, argOptions['max-levels'] || null);

        do {
            node.count++;
            node.size += size;
            node = node.parent;
        } while(node);
    });

    var outputTree = function(node, indent) {
        var str = "{SIZE: " + (node.size / 1e6).toFixed(2) + "MB, COUNT: " + node.count + ", AVG: " + (node.count ? (node.size / node.count) / 1e3 : 0).toFixed(2) + "KB";

        var size = _.size(node.children);
        if (size > 0) {
            str += "\n";
            var keys = _.keys(node.children).sort();
            _.each(keys, function(key) {
                var child = node.children[key];
                if (size < 100 || _.size(child.children)) {
                    str += indent + "  " + key + ": " + outputTree(child, "  " + indent) + "\n";
                }
            });
            str += indent + "}";
        } else {
            str += "}";
        }
        return str;
    };
    var str = outputTree(measurementsTree, "");
    console.log(str);


    console.log('output grouped');
};

/**
 * Starts sampling until all keys have been sampled.
 * @param cb
 */
var startSampling = function(cb) {
    var offset = 0;
    var sampleSize = argOptions['sample-size'] || 1000;

    var onUpdate = function() {
        var progress = "";
        if (allKeysFetched) {
            progress = "(" + (100 * totals.count / allKeys.length).toFixed(2) + "%) ";
        }
        console.log("Processed " + totals.count + " samples " + progress + "with total size " + (totals.size / 1e6).toFixed(2) + "MB (avg: " + ((totals.size / totals.count) / 1e3).toFixed(2) + "KB)");
    };

    var sampleLoop = function() {
        if (offset >= allKeys.length) {
            if (allKeysFetched) {
                // Done!
                cb();
            } else {
                // Wait for more keys.
                setTimeout(sampleLoop, 1000);
            }
        } else {
            var end = Math.min(allKeys.length, offset + sampleSize);
            doSample(allKeys, offset, end, function(err) {
                if (err) return cb(err);

                offset = end;

                onUpdate();
                sampleLoop();
            });
        }
    };

    sampleLoop();
};


var measurements = {};
var totals = {
    "size": 0,
    "count": 0
};

/**
 * Samples a bunch of keys for string length.
 * @param keys
 * @param offset
 * @param end
 * @param cb
 */
var doSample = function(keys, offset, end, cb) {
    // Get length.
    var tasks = [];
    for (var i = offset; i < end; i++) {
        var key = keys[i];
        (function(key) {
            tasks.push(function(cb) {
                redisClient.strlen(key, function(err, c) {
                    if (err) {
                        // Complex type: ignore.
                        cb(null, null);
                    } else {
                        cb(null, c + key.length);
                    }
                });

                if (argOptions['reset-expire']) {
                    var expire = parseInt(argOptions['reset-expire']);
                    if (expire > 0) {
                        redisClient.get(key, function(err, v) {
                            if (err) {
                                console.error(err);
                            } else {
                                redisClient.setex(key, expire, v, function(err) {
                                    if (err) console.error(err);
                                });
                            }
                        });
                    }
                }

                if (argOptions.hasOwnProperty('delete')) {
                    redisClient.del(key, function(err) {
                        if (err) console.error(err);
                    });
                }
            });
        })(key);
    }

    async.parallel(tasks, function(err, res) {
        if (err) return cb(err);
        _.each(res, function(count, index) {
            if (count !== null) {
                measurements[keys[index + offset]] = count;
                totals.size += count;
                totals.count++;
            }
        });

        cb();
    });
};

var allKeys = [];
var allKeysFetched = false;

/**
 * Returns all keys matching the specified pattern, without locking the database;
 * @param cb
 */
var getAllKeys = function(cb) {
    var re = null;
    if (realArgs[1]) {
        try {
            re = new RegExp(realArgs[1])
        } catch(e) {
            return cb(e);
        }
    }

    var lastN = Math.floor(allKeys.length / 1e5);
    var loopGetKeys = function() {
        getRandomKeys(realArgs[0], 10000, function(err, keys) {
            var newLastN = Math.floor(allKeys.length / 1e5);
            if (newLastN != lastN) {
                console.log('Fetched ' + allKeys.length + ' keys');
                lastN = newLastN;
            }

            if (err) return cb(err);

            keys.forEach(function(key) {
                if (!re || (re.test(key) == !argOptions.hasOwnProperty('regexp-inverse'))) {
                    allKeys.push(key);
                }
            });

            if (allKeysFetched) {
                console.log('Fetched ' + allKeys.length + ' keys');
                cb(null);
            } else {
                loopGetKeys();
            }
        });
    };
    loopGetKeys();
};

var scanIterator = 0;

/**
 * Returns a random bunch of keys matching the specified pattern.
 * @param pattern
 * @param count
 */
var getRandomKeys = function(pattern, count, cb) {
    redisClient.scan(scanIterator, "MATCH", pattern, "COUNT", count, function(err, res) {
        if (err) return cb(err);

        scanIterator = res[0];

        if (scanIterator == 0) {
            allKeysFetched = true;
        }

        cb(null, res[1]);
    });
} ;

exec(function(err) {
    if (err) {
        throw err;
        process.exit(1);
    }
});