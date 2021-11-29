const getAllEventData = require('getAllEventData');
const log = require("logToConsole");
const JSON = require("JSON");
const sendHttpRequest = require('sendHttpRequest');

log(data);

const postBody = JSON.stringify(getAllEventData());

log('postBody parsed to:', postBody);

const url = data.endpoint + '/' + data.topic_path

log('Sending event data to:' + url);

const options = {method: 'POST', 
                 headers: {'Content-Type':'application/json'}};

// Sends a POST request
sendHttpRequest(url, (statusCode) => {
  if (statusCode >= 200 && statusCode < 300) {
    data.gtmOnSuccess();
  } else {
    data.gtmOnFailure();
  }
}, options, postBody);
