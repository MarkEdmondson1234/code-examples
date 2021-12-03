/**
 * A transform function which filters out fields starting with x-ga
 * @param {string} inJson
 * @return {string} outJson
 */
 function transform(inJson) {
    var obj = JSON.parse(inJson);
    var keys = Object.keys(obj);
    var outJson = {};

    // don't output keys that starts with x-ga
    var outJson = keys.filter(function(key) {
        return !key.startsWith('x-ga');
    }).reduce(function(acc, key) {
        acc[key] = obj[key];
        return acc;
    }, {});
    
    return JSON.stringify(outJson);
  }