"use strict";

var Firebase = require("firebase");
var Promise = require('promise');
var _ = require('lodash');
var elasticsearch = require('elasticsearch');
var request = require('request');
var express = require('express');
var app = express();
var cors = require('cors');
var compression = require('compression');

app.use(cors());
app.use(compression());

var elasticsearchClient = new elasticsearch.Client({
  host: 'localhost:9200',
  log: 'error'
});

var topStoryItems = [];
var newStoryItems = [];
var askStoryItems = [];
var showStoryItems = [];
var jobStoryItems = [];

// respond with "hello world" when a GET request is made to the homepage
app.get('/', function(req, res) {
  res.send('hello world');
});

app.get('/hnItems', function(req, res) {
  var payload = {
    meta: {
      status: "ok",
      total: 0
    },
    hnItems: []
  };

  var searchBody = {
    query: {
      match_all: {}
    }
  };

  if (req.query.q) {
    searchBody.query = {
      multi_match: {
        query: req.query.q,
        fields: ["by", "text", "title"]
      }
    };
  }

  if (req.query.type) {
    searchBody = {
      query: {
        filtered: {
          query: searchBody.query,
          filter: {
            term: {
              type: req.query.type
            }
          }
        }
      }
    };
  }

  if (req.query.special) {
    switch (req.query.special) {
      case 'top':
        payload.hnItems = topStoryItems;
        payload.meta.total = topStoryItems.length;
        res.json(payload);
        break;
      case 'new':
        payload.hnItems = newStoryItems;
        payload.meta.total = newStoryItems.length;
        res.json(payload);
        break;
      case 'ask':
        payload.hnItems = askStoryItems;
        payload.meta.total = askStoryItems.length;
        res.json(payload);
        break;
      case 'show':
        payload.hnItems = showStoryItems;
        payload.meta.total = showStoryItems.length;
        res.json(payload);
        break;
      case 'job':
        payload.hnItems = jobStoryItems;
        payload.meta.total = jobStoryItems.length;
        res.json(payload);
        break;
      default:
        res.json(payload);
        break;
    }
  } else {
    elasticsearchClient.search({
      index: 'hn',
      type: 'item',
      size: 50,
      body: searchBody
    }, function(err, response) {
      // ...
      if (err) {
        console.log(err);
        res.json(payload);
      } else {
        if (response.hits.hits && response.hits.hits.length > 0) {
          payload.hnItems = _.map(response.hits.hits, function(hit) {
            return hit._source;
          });
        }
        res.json(payload);
      }
    });
  }
});

app.listen(5003);

var getLocalItem = function(itemId) {
  return new Promise(function(fulfill, reject) {
    elasticsearchClient.get({
      index: 'hn',
      type: 'item',
      id: itemId
    }, function(error, response) {
      if (error) {
        //console.log(error.message);
        reject(error.message);
      } else {
        //console.log(response);
        fulfill(response._source);
      }
    });
  });
};

var getLocalItems = function(itemIds) {
  return Promise.all(itemIds.map(getLocalItem));
};

var indexItem = function(item) {
  return new Promise(function(fulfill, reject) {
    elasticsearchClient.index({
      index: 'hn',
      type: 'item',
      id: item.id,
      body: item
    }, function(error, response) {
      if (error) {
        //console.log(error);
        reject(error.message);
      } else {
        //console.log(response);
        fulfill(response);
      }
    });
  });
};

var indexUser = function(user) {
  return new Promise(function(fulfill, reject) {
    elasticsearchClient.index({
      index: 'hn',
      type: 'user',
      id: user.id,
      body: user
    }, function(error, response) {
      if (error) {
        //console.log(error);
        reject(error.message);
      } else {
        //console.log(response);
        fulfill(response);
      }
    });
  });
};

var fetchItem = function(itemId) {
  return new Promise(function(fulfill, reject) {
    request('https://hacker-news.firebaseio.com/v0/item/' + itemId + '.json', function(error, response, body) {
      if (error) {
        //console.log(error);
        reject(error);
      } else {
        //console.log(body); // Show the HTML for the Google homepage.
        fulfill(indexItem(JSON.parse(body)));
      }
    });
  });
};

var fetchUser = function(username) {
  return new Promise(function(fulfill, reject) {
    request('https://hacker-news.firebaseio.com/v0/user/' + username + '.json', function(error, response, body) {
      if (error) {
        //console.log(error);
        reject(error);
      } else {
        //console.log(body); // Show the HTML for the Google homepage.
        fulfill(indexUser(JSON.parse(body)));
      }
    });
  });
};

var fetchItems = function(itemIds) {
  if (_.isArray(itemIds) && itemIds.length > 0) {
    var fetchId = itemIds.shift();
    fetchItem(fetchId)
      .catch(function(err) {
        console.log('Failed to fetched item ' + fetchId + ': ' + err);
        fetchItems(itemIds);
        //reject('Failed to fetched item ' + itemId + ': ' + err);
      })
      .then(function() {
        //console.log('Fetched item ' + fetchId + ' successfully.');
        fetchItems(itemIds);
        //fulfill('Fetched item ' + itemId + ' successfully.');
      });
  }
};

var fetchUsers = function(usernames) {
  if (_.isArray(usernames) && usernames.length > 0) {
    var fetchUsername = usernames.shift();
    fetchUser(fetchUsername)
      .catch(function(err) {
        console.log('Failed to fetched item ' + fetchUsername + ': ' + err);
        fetchUsers(usernames);
        //reject('Failed to fetched item ' + itemId + ': ' + err);
      })
      .then(function() {
        //console.log('Fetched item ' + fetchId + ' successfully.');
        fetchUsers(usernames);
        //fulfill('Fetched item ' + itemId + ' successfully.');
      });
  }
};

var fetchNewItems = function(itemIds) {
  if (_.isArray(itemIds) && itemIds.length > 0) {
    var fetchId = itemIds.shift();
    getLocalItem(fetchId)
      .catch(function() {
        fetchItem(fetchId)
          .catch(function() {
            fetchNewItems(itemIds);
            //reject('Failed to fetched item ' + itemId + ': ' + err);
          })
          .then(function() {
            fetchNewItems(itemIds);
            //fulfill('Fetched item ' + itemId + ' successfully.');
          });
      })
      .then(function() {
        fetchNewItems(itemIds);
        //fulfill('Fetched item ' + itemId + ' successfully.');
      });
  }
};

var walkItems = function(itemId) {
  fetchItem(itemId)
    .catch(function(err) {
      console.log('Failed to fetched item ' + itemId + ': ' + err);
      if (itemId > 1) {
        walkItems(itemId - 1);
      }
      //reject('Failed to fetched item ' + itemId + ': ' + err);
    })
    .then(function() {
      //console.log('Fetched item ' + itemId + ' successfully.');
      if (itemId > 1) {
        walkItems(itemId - 1);
      }
      //fulfill('Fetched item ' + itemId + ' successfully.');
    });
};

var maxitemid = new Firebase("https://hacker-news.firebaseio.com/v0/maxitem");
var topstories = new Firebase("https://hacker-news.firebaseio.com/v0/topstories");
var newstories = new Firebase("https://hacker-news.firebaseio.com/v0/newstories");
var askstories = new Firebase("https://hacker-news.firebaseio.com/v0/askstories");
var showstories = new Firebase("https://hacker-news.firebaseio.com/v0/showstories");
var jobstories = new Firebase("https://hacker-news.firebaseio.com/v0/jobstories");
var changes = new Firebase("https://hacker-news.firebaseio.com/v0/updates");

maxitemid.once("value", function(snapshot) {
  elasticsearchClient.search({
    index: 'hn',
    type: 'item',
    sort: 'time:asc',
    size: 1
  }, function(err, response) {
    // ...
    if (err) {
      console.log(err);
      console.log('Walking items from ' + snapshot.val());
      walkItems(snapshot.val());
    } else {
      if (response.hits.hits && response.hits.hits.length > 0) {
        console.log(response.hits.hits[0]);
        if (response.hits.hits[0].id > 1) {
          console.log('Walking items from ' + response.hits.hits[0].id);
          walkItems(response.hits[0].id);
        } else {
          console.log('Walking items from ' + snapshot.val());
          walkItems(snapshot.val());
        }
      }
    }
  });
  //console.log(snapshot.val());
}, function(errorObject) {
  console.log("The read failed: " + errorObject.code);
});

topstories.on("value", function(snapshot) {
  var top = snapshot.val();
  fetchNewItems(top);
  getLocalItems(top)
    .catch(function(err) {
      console.log('Error finding all top items: ' + err);
    })
    .then(function(items) {
      //console.log('Top items found.');
      if (_.isArray(items)) {
        topStoryItems = items;
      }
    });
  //console.log(snapshot.val());
}, function(error) {
  console.log(error);
  //console.log("The read failed: " + errorObject.code);
});

newstories.on("value", function(snapshot) {
  var top = snapshot.val();
  fetchNewItems(top);
  getLocalItems(top)
    .catch(function(err) {
      console.log('Error finding all new items: ' + err);
    })
    .then(function(items) {
      //console.log('Top items found.');
      if (_.isArray(items)) {
        newStoryItems = items;
      }
    });
  //console.log(snapshot.val());
}, function(error) {
  console.log(error);
  //console.log("The read failed: " + errorObject.code);
});

askstories.on("value", function(snapshot) {
  var top = snapshot.val();
  fetchNewItems(top);
  getLocalItems(top)
    .catch(function(err) {
      console.log('Error finding all ask items: ' + err);
    })
    .then(function(items) {
      //console.log('Top items found.');
      if (_.isArray(items)) {
        askStoryItems = items;
      }
    });
  //console.log(snapshot.val());
}, function(error) {
  console.log(error);
  //console.log("The read failed: " + errorObject.code);
});

showstories.on("value", function(snapshot) {
  var top = snapshot.val();
  fetchNewItems(top);
  getLocalItems(top)
    .catch(function(err) {
      console.log('Error finding all show items: ' + err);
    })
    .then(function(items) {
      //console.log('Top items found.');
      if (_.isArray(items)) {
        showStoryItems = items;
      }
    });
  //console.log(snapshot.val());
}, function(error) {
  console.log(error);
  //console.log("The read failed: " + errorObject.code);
});

jobstories.on("value", function(snapshot) {
  var top = snapshot.val();
  fetchNewItems(top);
  getLocalItems(top)
    .catch(function(err) {
      console.log('Error finding all job items: ' + err);
    })
    .then(function(items) {
      //console.log('Top items found.');
      if (_.isArray(items)) {
        jobStoryItems = items;
      }
    });
  //console.log(snapshot.val());
}, function(error) {
  console.log(error);
  //console.log("The read failed: " + errorObject.code);
});

changes.on("value", function(snapshot) {
  var updates = snapshot.val();
  console.log('Fetching updates');
  fetchItems(updates.items);
  fetchUsers(updates.profiles);
  //console.log(snapshot.val());
}, function(error) {
  console.log(error);
  //console.log("The read failed: " + errorObject.code);
});