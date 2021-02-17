'use strict';
/*
  Local elasticsearch client
*/
// const client = new elasticsearch.Client({
//   host: 'localhost:9200'
// });
const { Client } = require('@elastic/elasticsearch');
const client = new Client({ node: 'http://localhost:9200' });

/**
 * Gets hn item from elastic search
 * @param  {int} itemId
 * @return {Promise}        promise returning item
 */
async function getItem(itemId) {
  return client.get({
    index: 'hn',
    type: 'item',
    id: itemId
  });
  // return new Promise(function (resolve, reject) {
  //   elasticsearchClient.get({
  //     index: 'hn',
  //     type: 'item',
  //     id: itemId
  //   }, function (error, response) {
  //     if (error) {
  //       console.log(error.message);
  //       reject(error.message);
  //     } else {
  //       console.log(response);
  //       resolve(response._source);
  //     }
  //   });
  // });
};

/**
 * Gets hn item from elastic search
 * @param  {int[]} itemIds
 * @return {Promise}        promise returning array of items
 */
async function getItems(itemIds) {
  const results = await client.mget({
    index: 'hn',
    type: 'item',
    body: {
      ids: itemIds
    }
  });
  if(results.body) {
    return results.body.docs;
  } else return [];

  // return new Promise(function (resolve, reject) {
  //   elasticsearchClient.mget({
  //     index: 'hn',
  //     type: 'item',
  //     body: {
  //       ids: itemIds
  //     }
  //   }, function (err, response) {
  //     if (err) {
  //       console.log(err);
  //       reject(err.message);
  //       //res.json(payload);
  //     } else {
  //       console.log(response);
  //       if (response.docs && response.docs.length > 0) {
  //         resolve(_.map(response.docs, function (hit) {
  //           return hit._source;
  //         }));
  //       } else {
  //         reject("No items");
  //       }
  //     }
  //   });
  // });
};

/**
 * Indexs hn item into elastic
 * @param  {Object} item
 * @return {Promise}      returns elastic response
 */
async function indexItem(item) {
  return client.index({
    index: 'hn',
    refresh: true,
    id: item.id,
    body: item
  });
  // return new Promise(function (resolve, reject) {
  //   elasticsearchClient.index({
  //     index: 'hn',
  //     type: 'item',
  //     id: item.id,
  //     body: item
  //   }, function (error, response) {
  //     if (error) {
  //       console.log(error);
  //       reject(error.message);
  //     } else {
  //       console.log(response);
  //       resolve(response);
  //     }
  //   });
  // });
};

/**
 * Indexs hn user into elastic
 * @param  {Object} user
 * @return {Promise}      returns elastic response
 */
async function indexUser(user) {
  user.username = user.id;
  delete user.id;

  try {
    return client.index({
      index: 'hn',
      refresh: true,
      id: user.username,
      body: user,
    });
  } catch (e) {
    console.error({ e });
    console.error({
      index: 'hn',
      refresh: true,
      id: user.username,
      body: user,
    });
  }

  // return new Promise(function(resolve, reject) {
  //   user.username = user.id;
  //   delete user.id;
  //   elasticsearchClient.index({
  //     index: 'hn',
  //     type: 'user',
  //     id: user.username,
  //     body: user
  //   }, function(error, response) {
  //     if (error) {
  //       console.log(error);
  //       reject(error.message);
  //     } else {
  //       console.log(response);
  //       resolve(response);
  //     }
  //   });
  // });
};

async function search(params) {
  return client.search(params);
}

module.exports = {
  getItem,
  getItems,
  indexItem,
  indexUser,
  search,
};
