'use strict';
/*
  External npm modules
*/
const _ = require('lodash');
const firebase = require("firebase");
const express = require('express');

const es = require('./es');

/*
  Firebase clients
*/
firebase.initializeApp({
  databaseURL: "https://hacker-news.firebaseio.com",
});

const hnDb = firebase.database();

const maxitemid = hnDb.ref('v0/maxitem');
const topstories = hnDb.ref('v0/topstories');
const newstories = hnDb.ref('v0/newstories');
const askstories = hnDb.ref('v0/askstories');
const showstories = hnDb.ref('v0/showstories');
const jobstories = hnDb.ref('v0/jobstories');
const updates = hnDb.ref('v0/updates');

/*
  Variables to hold special cases.
  Either this or reverse engineer HN's ranking algorithms.
  Probably only needed for top articles, may change in the future
*/
let topStoryIds = [];
let newStoryIds = [];
let askStoryIds = [];
let showStoryIds = [];
let jobStoryIds = [];

let topStoryItems = [];
let newStoryItems = [];
let askStoryItems = [];
let showStoryItems = [];
let jobStoryItems = [];

/*
  Server config
*/
let pageSize = 30;

/*
  Helper functions to fetch and store data
*/

/**
 * Fetchs hn item from firebase
 * @param  {int} itemId
 * @return {Promise}        promise returning item
 */
async function fetchItem(itemId) {
  // console.log('Fetching item ' + itemId);
  try {
    let snapshot = await hnDb.ref('v0/item/' + itemId).once('value');
    if (snapshot.exists()) {
      // console.log(snapshot.val());
      return es.indexItem(snapshot.val());
    } else {
      console.error(`Item ${itemId}: No data available`);
    }
  } catch (error) {
    console.error(`fetchItem: ${itemId} failed`);
    console.error(error);
  }
  return null;
};

/**
 * Fetchs hn user from firebase
 * @param  {String} username
 * @return {Promise}        promise returning user
 */
// async function fetchUser(username) {
//   // console.log('Fetching user ' + username);
//   try {
//     let snapshot = await hnDb.ref('v0/user/' + username).once('value');
//     if (snapshot.exists()) {
//       // console.log(snapshot.val());
//       return es.indexUser(snapshot.val());
//     }
//     else {
//       console.error("No data available");
//     }
//   } catch (error) {
//     console.error(`fetchItem: ${itemId} failed`)
//     console.error(error);
//   }
//   return null;
// };

/**
 * Fetchs hn items from firebase
 * @param  {int[]} itemIds
 * @return {Promise}        promise returning items
 */
async function fetchItems(itemIds) {
  if (_.isArray(itemIds) && itemIds.length > 0) {
    return Promise.all(itemIds.map(fetchItem));
  }
  return null;
};

/**
 * Fetchs hn users from firebase
 * @param  {String[]} usernames
 * @return {Promise}        promise returning user
 */
async function fetchUsers(usernames) {
  if (_.isArray(usernames) && usernames.length > 0) {
    return Promise.all(usernames.map(fetchUser));
  }
  return null;
  // let fetchUsername = usernames.shift();
  // fetchUser(fetchUsername)
  //   .catch(function (err) {
  //     // console.log('Failed to fetched user ' + fetchUsername + ': ' + err);
  //     fetchUsers(usernames);
  //     //reject('Failed to fetched item ' + itemId + ': ' + err);
  //   })
  //   .then(function () {
  //     // console.log('Fetched item ' + fetchId + ' successfully.');
  //     fetchUsers(usernames);
  //     //resolve('Fetched item ' + itemId + ' successfully.');
  //   });
};

async function fetchNewItems(itemIds, oldItemIds) {
  let idsToFetch = [];
  if (oldItemIds && _.isArray(oldItemIds)) {
    if (_.isArray(itemIds) && itemIds.length > 0) {
      idsToFetch = _.difference(itemIds, oldItemIds);
    }
  }

  return Promise.all(idsToFetch.map(fetchItem));
};

// function walkItems(startItemId, stopItemId) {
//   if (!stopItemId) stopItemId = 1;
//   fetchItem(startItemId)
//     .catch(function (err) {
//       // console.log('Failed to fetched item ' + startItemId + ': ' + err);
//       if (startItemId > stopItemId) {
//         walkItems(startItemId - 1, stopItemId);
//       }
//       //reject('Failed to fetched item ' + startItemId + ': ' + err);
//     })
//     .then(function () {
//       // console.log('Fetched item ' + startItemId + ' successfully.');
//       if (startItemId > stopItemId) {
//         walkItems(startItemId - 1, stopItemId);
//       }
//       //resolve('Fetched item ' + itemId + ' successfully.');
//     });
// };

function formatGetItemResults(items) {
  return _.chunk(
    _.without(
      items,
      null
    ).map(({ _source }) => _source),
    pageSize
  );
}

topstories.on("value", async function (snapshot) {
  const ids = snapshot.val();
  await fetchNewItems(ids, topStoryIds);
  topStoryIds = ids;
  try {
    const items = await es.getItems(ids);
    if (_.isArray(items)) {
      topStoryItems = formatGetItemResults(items);
      topStoryItems.push(items.length);
    }
  } catch (e) {
    console.error(e);
  }
}, function (error) {
  console.error(error);
  console.error("The read failed: " + error.code);
});

newstories.on("value", async function (snapshot) {
  const ids = snapshot.val();
  await fetchNewItems(ids, topStoryIds);
  newStoryIds = ids;
  try {
    const items = await es.getItems(ids);
    if (_.isArray(items)) {
      newStoryItems = formatGetItemResults(items);
      newStoryItems.push(items.length);
    }
  } catch (e) {
    console.error(e);
  }
  // console.log('newstoriesvalue');
  // let ids = snapshot.val();
  // fetchNewItems(ids, newStoryIds).then(function () {
  //   newStoryIds = ids;
  //   es.getItems(ids)
  //     .catch(function (err) {
  //       // console.log('Error finding all top items: ' + err);
  //     })
  //     .then(function (items) {
  //       // console.log('Top items found.');
  //       if (_.isArray(items)) {
  //         newStoryItems = _.chunk(_.without(items, null), pageSize);
  //         // console.log(topStoryItems);
  //         newStoryItems.push(items.length);
  //         // console.log(topStoryItems);
  //       }
  //     });
  // });
}, function (error) {
  console.error(error);
  console.error("The read failed: " + error.code);
});

askstories.on("value", async function (snapshot) {
  const ids = snapshot.val();
  await fetchNewItems(ids, topStoryIds);
  askStoryIds = ids;
  try {
    const items = await es.getItems(ids);
    if (_.isArray(items)) {
      askStoryItems = formatGetItemResults(items);
      askStoryItems.push(items.length);
    }
  } catch (e) {
    console.error(e);
  }
  // // console.log('askstoriesvalue');
  // let ids = snapshot.val();
  // fetchNewItems(ids, askStoryIds).then(function () {
  //   askStoryIds = ids;
  //   es.getItems(ids)
  //     .catch(function (err) {
  //       // console.log('Error finding all top items: ' + err);
  //     })
  //     .then(function (items) {
  //       // console.log('Top items found.');
  //       if (_.isArray(items)) {
  //         askStoryItems = _.chunk(_.without(items, null), pageSize);
  //         // console.log(topStoryItems);
  //         askStoryItems.push(items.length);
  //         // console.log(topStoryItems);
  //       }
  //     });
  // });
}, function (error) {
  console.error(error);
  console.error("The read failed: " + error.code);
});

showstories.on("value", async function (snapshot) {
  const ids = snapshot.val();
  await fetchNewItems(ids, topStoryIds);
  showStoryIds = ids;
  try {
    const items = await es.getItems(ids);
    if (_.isArray(items)) {
      showStoryItems = formatGetItemResults(items);
      showStoryItems.push(items.length);
    }
  } catch (e) {
    console.error(e);
  }
  // let ids = snapshot.val();
  // fetchNewItems(ids, showStoryIds).then(function () {
  //   showStoryIds = ids;
  //   es.getItems(ids)
  //     .catch(function (err) {
  //       // console.log('Error finding all top items: ' + err);
  //     })
  //     .then(function (items) {
  //       // console.log('Top items found.');
  //       if (_.isArray(items)) {
  //         showStoryItems = _.chunk(_.without(items, null), pageSize);
  //         // console.log(topStoryItems);
  //         showStoryItems.push(items.length);
  //         // console.log(topStoryItems);
  //       }
  //     });
  // });
}, function (error) {
  console.error(error);
  console.error("The read failed: " + error.code);
});

jobstories.on("value", async  function (snapshot) {
  const ids = snapshot.val();
  await fetchNewItems(ids, topStoryIds);
  jobStoryIds = ids;
  try {
    const items = await es.getItems(ids);
    if (_.isArray(items)) {
      jobStoryItems = formatGetItemResults(items);
      jobStoryItems.push(items.length);
    }
  } catch (e) {
    console.error(e);
  }
  // let ids = snapshot.val();
  // fetchNewItems(ids, jobStoryIds).then(function () {
  //   jobStoryIds = ids;
  //   es.getItems(ids)
  //     .catch(function (err) {
  //       // console.log('Error finding all top items: ' + err);
  //     })
  //     .then(function (items) {
  //       // console.log('Top items found.');
  //       if (_.isArray(items)) {
  //         jobStoryItems = _.chunk(_.without(items, null), pageSize);
  //         // console.log(topStoryItems);
  //         jobStoryItems.push(items.length);
  //         // console.log(topStoryItems);
  //       }
  //     });
  // });
}, function (error) {
  console.error(error);
  console.error("The read failed: " + error.code);
});

updates.on("value", async function (snapshot) {
  let updates = snapshot.val();
  // console.log('Fetching updates');
  // console.log(snapshot.val());
  try {
    await fetchItems(updates.items);
  } catch(e) {
    console.error(e);
  }
  // try {
  //   await fetchUsers(updates.profiles);
  // } catch(e) {
  //   console.error(e);
  // }
}, function (error) {
  console.error(error);
  console.error("The read failed: " + error.code);
});

/*
  Express server to seve Hacker News data from ElasticSearch
*/
const app = express();

// respond with "hello world" when a GET request is made to the homepage
app.get('/', function (req, res) {
  res.send('hello world');
});

app.get('/hnItems', async (req, res) => {
  let payload = {
    meta: {
      status: "ok",
      total: 0,
      pageSize: pageSize
    },
    hnItems: []
  };

  let searchBody = {
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
        bool: {
          must: searchBody.query,
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
        bool: {
          must: searchBody.query,
          filter: {
            term: {
              type: 'story'
            }
          }
        }
      }
    };
  }

  if (req.query.sort) {
    searchBody.sort = [{
      "time": {
        "order": "desc"
      }
    }];
  } else {
    searchBody.sort = [{
      "time": {
        "order": "desc"
      }
    }];
  }

  if (req.query.page && req.query.page > 0) {
    req.query.page = Math.ceil(req.query.page);
  } else {
    req.query.page = 1;
  }

  payload.meta.page = req.query.page;

  let sendSpecial = function (items) {
    // console.log('special');
    payload.meta.total = items[items.length - 1];
    payload.meta.pageTotal = items.length - 1;
    if (payload.meta.page < items.length) {
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
    try {
      // console.log()
      const { body } = await es.search({
        index: 'hn',
        size: pageSize,
        from: pageSize * (payload.meta.page - 1),
        body: searchBody
      });
      // console.log(body);
      if (body.hits.hits && body.hits.hits.length > 0) {
        payload.meta.total = body.hits.total;
        payload.meta.pageTotal = Math.ceil(body.hits.total / pageSize);

        payload.hnItems = _.map(body.hits.hits, function (hit) {
          return hit._source;
        });
      }
      res.json(payload);
    } catch (e) {
      console.error(JSON.stringify(e, null, 2));
      res.json(payload);
    }
  }
});
// console.log('huh');
app.listen(5003);
