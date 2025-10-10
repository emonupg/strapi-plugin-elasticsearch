const configureIndexingRoutes = require('./configure-indexing');
const performSearch = require('./perform-search');
const runLog = require('./run-log');
const setupInfo = require('./setup-info');
const performIndexing = require('./perform-indexing');
const performIndexingViaFrontend = require('./perform-indexing-via-frontend');
module.exports = {
  config: configureIndexingRoutes,
  search: performSearch,
  runLog: runLog,
  setupInfo: setupInfo,
  performIndexing: performIndexing,
  performIndexingViaFrontend: performIndexingViaFrontend
};
