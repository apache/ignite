import * as utils from '../utils.js';

const routes = function () {
  const exp = {};

  exp.viewDatabase = async function (req, res) {
    await req.updateCollections(req.dbConnection).then(async () => {
      await req.db.stats().then((data) => {
        const ctx = {
          title: 'Viewing Database: ' + req.dbName,
          databases: req.databases,
          colls: req.collections[req.dbName],
          grids: req.gridFSBuckets[req.dbName],
          stats: {
            avgObjSize: utils.bytesToSize(data.avgObjSize || 0),
            collections: data.collections,
            dataFileVersion: (data.dataFileVersion && data.dataFileVersion.major && data.dataFileVersion.minor
              ? data.dataFileVersion.major + '.' + data.dataFileVersion.minor
              : null),
            dataSize: utils.bytesToSize(data.dataSize),
            extentFreeListNum: (data.extentFreeList && data.extentFreeList.num ? data.extentFreeList.num : null),
            fileSize: (data.fileSize === undefined ? null : utils.bytesToSize(data.fileSize)),
            indexes: data.indexes,
            indexSize: utils.bytesToSize(data.indexSize),
            numExtents: (data.numExtents ? data.numExtents.toString() : null),
            objects: data.objects,
            storageSize: utils.bytesToSize(data.storageSize),
          },
        };
        res.render('database', ctx);
      }).catch((error) => {
        req.session.error = 'Could not get stats. ' + JSON.stringify(error);
        console.error(error);
        res.redirect('back');
      });
    }).catch((error) => {
      req.session.error = 'Could not refresh collections. ' + JSON.stringify(error);
      console.error(error);
      res.redirect('back');
    });
  };

  exp.addDatabase = async function (req, res) {
    const name = req.body.database;
    if (!utils.isValidDatabaseName(name)) {
      // TODO: handle error
      console.error('That database name is invalid.');
      req.session.error = 'That database name is invalid.';
      return res.redirect('back');
    }
    const ndb = req.mainClient.client.db(name);

    await ndb.createCollection('delete_me').then(async () => {
      await req.updateDatabases().then(() => {
        res.redirect(res.locals.baseHref);
      });

      // await ndb.dropCollection('delete_me').then(() => {
      //   res.redirect(res.locals.baseHref + 'db/' + name);
      // }).catch((error) => {
      //   //TODO: handle error
      //   console.error('Could not delete collection.');
      //   req.session.error = 'Could not delete collection. Err:' + error;
      //   res.redirect('back');
      // });
    }).catch((error) => {
      // TODO: handle error
      console.error('Could not create collection. Err:' + error);
      req.session.error = 'Could not create collection. Err:' + error;
      res.redirect('back');
    });
  };

  exp.deleteDatabase = async function (req, res) {
    await req.db.dropDatabase().then(async () => {
      await req.updateDatabases().then(() => res.redirect(res.locals.baseHref));
    }).catch((error) => {
      // TODO: handle error
      console.error('Could not to delete database. Err:' + error);
      req.session.error = 'Failed to delete database. Err:' + error;
      res.redirect('back');
    });
  };

  return exp;
};

export default routes;
