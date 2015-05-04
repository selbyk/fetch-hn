"use strict";

/*
  External npm modules
*/
var Firebase = require("firebase");
var Promise = require('promise');
var _ = require('lodash');
var elasticsearch = require('elasticsearch');
var request = require('request');
var express = require('express');
var app = express();
var cors = require('cors');
var compression = require('compression');

/*
  Local elasticsearch client
*/
var elasticsearchClient = new elasticsearch.Client({
  host: 'localhost:9200',
  log: 'error'
});

/*
  Firebase clients
*/
var maxitemid = new Firebase("https://hacker-news.firebaseio.com/v0/maxitem");
var topstories = new Firebase("https://hacker-news.firebaseio.com/v0/topstories");
var newstories = new Firebase("https://hacker-news.firebaseio.com/v0/newstories");
var askstories = new Firebase("https://hacker-news.firebaseio.com/v0/askstories");
var showstories = new Firebase("https://hacker-news.firebaseio.com/v0/showstories");
var jobstories = new Firebase("https://hacker-news.firebaseio.com/v0/jobstories");
var changes = new Firebase("https://hacker-news.firebaseio.com/v0/updates");

/*
  Variables to hold special cases.
  Either this or reverse engineer HN's ranking algorithms.
  Probably only needed for top articles, may change in the future
*/
var topStoryItems = [];
var newStoryItems = [];
var askStoryItems = [];
var showStoryItems = [];
var jobStoryItems = [];

/*
  Server config
*/
var pageSize = 30;

/*
  Helper functions to fetch and store data
*/
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
  return new Promise(function(fulfill, reject) {
    elasticsearchClient.mget({
      index: 'hn',
      type: 'item',
      body: {
        ids: itemIds
      }
    }, function(err, response) {
      if (err) {
        //console.log(err);
        reject(err.message);
        //res.json(payload);
      } else {
        //console.log(response);
        if (response.docs && response.docs.length > 0) {
          fulfill(_.map(response.docs, function(hit) {
            return hit._source;
          }));
        } else {
          reject("No items");
        }
      }
    });
  });
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
  console.log('Fetching item ' + itemId);
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


/*
  Fetch Hacker News data from Firebase
*/
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
        //console.log(response.hits.hits[0]);
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
  console.log('Fetching top');
  var top = snapshot.val();
  fetchNewItems(top);
  //topStoryItems = _.chunk(top, 30);
  getLocalItems(top)
    .catch(function(err) {
      console.log('Error finding all top items: ' + err);
    })
    .then(function(items) {
      //console.log('Top items found.');
      if (_.isArray(items)) {
        topStoryItems = _.chunk(items, 30);
        //console.log(topStoryItems);
        topStoryItems.push(items.length);
        //console.log(topStoryItems);
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
        newStoryItems = _.chunk(items, 30);
        newStoryItems.push(items.length);
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
        askStoryItems = _.chunk(items, 30);
        askStoryItems.push(items.length);
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
        showStoryItems = _.chunk(items, 30);
        showStoryItems.push(items.length);
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
        jobStoryItems = _.chunk(items, 30);
        jobStoryItems.push(items.length);
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


/*
  Express server to seve Hacker News data from ElasticSearch
*/
app.use(cors());
app.use(compression());

// respond with "hello world" when a GET request is made to the homepage
app.get('/', function(req, res) {
  res.send('hello world');
});

app.get('/hnItems', function(req, res) {
  var payload = {
    meta: {
      status: "ok",
      total: 0,
      pageSize: pageSize
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
  } else {
    searchBody = {
      query: {
        filtered: {
          query: searchBody.query,
          filter: {
            term: {
              type: 'story'
            }
          }
        }
      }
    };
  }

  if (req.query.page && req.query.page > 0) {
    req.query.page = Math.ceil(req.query.page);
  } else {
    req.query.page = 1;
  }

  payload.meta.page = req.query.page;

  var sendSpecial = function(items) {
    payload.meta.total = items[items.length - 1];
    payload.meta.pageTotal = items.length - 1;
    if (payload.meta.page < items.length - 1) {
      payload.hnItems = items[payload.meta.page - 1];
    } else {
      payload.meta.page = 1;
      payload.hnItems = items[0];
    }
    res.json(payload);
  };

  if (req.query.special) {
    switch (req.query.special) {
      case 'top':
        sendSpecial(topStoryItems);
        break;
      case 'new':
        sendSpecial(newStoryItems);
        break;
      case 'ask':
        sendSpecial(askStoryItems);
        break;
      case 'show':
        sendSpecial(showStoryItems);
        break;
      case 'job':
        sendSpecial(jobStoryItems);
        break;
      default:
        res.json(payload);
        break;
    }
  } else {
    elasticsearchClient.search({
      index: 'hn',
      type: 'item',
      size: pageSize,
      from: pageSize * (payload.meta.page - 1),
      body: searchBody
    }, function(err, response) {
      // ...
      if (err) {
        console.log(err);
        res.json(payload);
      } else {
        if (response.hits.hits && response.hits.hits.length > 0) {
          payload.meta.total = response.hits.total;
          payload.meta.pageTotal = Math.ceil(response.hits.total / pageSize);

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