'use strict';

module.exports = ({ strapi }) => {
  const indexer = strapi.plugins['elasticsearch'].services.indexer;
  const scheduleIndexingService = strapi.plugins['elasticsearch'].services.scheduleIndexing;
  const rebuildIndex = async (ctx) => {
    return await indexer.rebuildIndex();
  };

  const indexCollection = async (ctx) => {
    if (ctx.params.collectionname)
      return await scheduleIndexingService.addCollectionToIndex({
        collectionUid: ctx.params.collectionname,
      });
    else return null;
  };

  const rebuildCollectionIndex = async (ctx) => {
    if (ctx.params.collectionname) {
      try {
        const result = await indexer.rebuildCollectionIndex(ctx.params.collectionname);
        return result;
      } catch (err) {
        console.error('strapi-plugin-elasticsearch : Error rebuilding collection index');
        console.error(err);
        ctx.throw(500, err.message || 'Failed to rebuild collection index');
      }
    } else {
      ctx.throw(400, 'Collection name is required');
    }
  };

  const triggerIndexing = async (ctx) => {
    return await indexer.indexPendingData();
  };

  const triggerIndexingTask = async (ctx) => {
    return await indexer.indexPendingData();
  };

  return {
    rebuildIndex,
    indexCollection,
    rebuildCollectionIndex,
    triggerIndexingTask,
    triggerIndexing,
  };
};
