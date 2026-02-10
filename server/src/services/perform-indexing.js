module.exports = ({ strapi }) => ({
  async rebuildCollectionIndex(collectionName) {
    const helper = strapi.plugins['elasticsearch'].services.helper;
    const esInterface = strapi.plugins['elasticsearch'].services.esInterface;
    const configureIndexingService = strapi.plugins['elasticsearch'].services.configureIndexing;
    const logIndexingService = strapi.plugins['elasticsearch'].services.logIndexing;
    const rebuildValidator = strapi.plugins['elasticsearch'].services.rebuildValidator;

    try {
      console.log(
        'strapi-plugin-elasticsearch : Request to rebuild index for collection:',
        collectionName
      );

      const model = strapi.getModel(collectionName);
      if (!model) {
        const errorMsg = `Collection ${collectionName} does not exist in Strapi. It may have been deleted.`;
        console.log('strapi-plugin-elasticsearch :', errorMsg);
        throw new Error(errorMsg);
      }

      const collectionConfig = await configureIndexingService.getCollectionConfig({
        collectionName,
      });
      if (!collectionConfig || Object.keys(collectionConfig).length === 0) {
        throw new Error(`Collection ${collectionName} is not configured for indexing`);
      }

      const oldIndexName = await helper.getCurrentIndexName(collectionName);
      console.log('strapi-plugin-elasticsearch : Current index:', oldIndexName);

      const newIndexName = await helper.getIncrementedIndexName(collectionName);
      await esInterface.createIndex(newIndexName);
      console.log('strapi-plugin-elasticsearch : Created new index:', newIndexName);

      await this.indexCollection(collectionName, newIndexName);
      console.log('strapi-plugin-elasticsearch : Indexed all data into new index');

      const validation = await rebuildValidator.validateRebuild({
        collectionName,
        indexName: newIndexName,
      });

      if (!validation.success) {
        console.log('strapi-plugin-elasticsearch : Validation failed:', validation);
        await logIndexingService.recordIndexingFail(
          `Rebuild validation failed for ${collectionName}`
        );
        return {
          success: false,
          collectionName,
          error: 'Validation failed',
          validation,
        };
      }

      await esInterface.updateContentTypeAlias({
        collectionName,
        newIndexName,
        oldIndexName,
      });
      console.log('strapi-plugin-elasticsearch : Updated content-type alias');

      const version = parseInt(newIndexName.match(/_(\d+)$/)?.[1] || '1');
      await helper.storeIndexInfo({
        collectionName,
        indexName: newIndexName,
        version,
      });

      try {
        await esInterface.deleteIndex(oldIndexName);
        console.log('strapi-plugin-elasticsearch : Deleted old index:', oldIndexName);
      } catch (err) {
        console.log(
          'strapi-plugin-elasticsearch : Could not delete old index (may not exist):',
          err.message
        );
      }

      await esInterface.updateGlobalSearchAlias();
      console.log('strapi-plugin-elasticsearch : Updated global search alias');

      await logIndexingService.recordIndexingPass(
        `Rebuild of collection ${collectionName} completed successfully.`
      );

      return {
        success: true,
        collectionName,
        newIndexName,
        oldIndexName,
        validation,
      };
    } catch (err) {
      console.log('strapi-plugin-elasticsearch : Error rebuilding collection index');
      console.log(err);
      await logIndexingService.recordIndexingFail(
        `Rebuild of collection ${collectionName} failed: ${err.message}`
      );
      throw err;
    }
  },

  async rebuildIndex() {
    const configureIndexingService = strapi.plugins['elasticsearch'].services.configureIndexing;
    const logIndexingService = strapi.plugins['elasticsearch'].services.logIndexing;

    try {
      console.log('strapi-plugin-elasticsearch : Request to rebuild all indices received.');

      const collections = await configureIndexingService.getCollectionsConfiguredForIndexing();
      const results = [];

      for (const collectionName of collections) {
        try {
          const result = await this.rebuildCollectionIndex(collectionName);
          results.push(result);
        } catch (err) {
          console.log(
            `strapi-plugin-elasticsearch : Failed to rebuild ${collectionName}:`,
            err.message
          );
          results.push({
            success: false,
            collectionName,
            error: err.message,
          });
        }
      }

      const successCount = results.filter((r) => r.success).length;
      const failCount = results.length - successCount;

      if (failCount === 0) {
        await logIndexingService.recordIndexingPass(
          `Successfully rebuilt ${successCount} collection indices.`
        );
        return { success: true, results };
      } else {
        await logIndexingService.recordIndexingFail(
          `Rebuilt ${successCount} collections, ${failCount} failed.`
        );
        return { success: false, results };
      }
    } catch (err) {
      console.log('strapi-plugin-elasticsearch : Error in rebuildIndex');
      console.log(err);
      await logIndexingService.recordIndexingFail(err.message);
      throw err;
    }
  },

  async indexCollection(collectionName, indexName = null) {
    const helper = strapi.plugins['elasticsearch'].services.helper;
    const populateAttrib = helper.getPopulateForACollection({ collectionName });
    const isCollectionDraftPublish = helper.isCollectionDraftPublish({ collectionName });
    const configureIndexingService = strapi.plugins['elasticsearch'].services.configureIndexing;
    const esInterface = strapi.plugins['elasticsearch'].services.esInterface;

    if (indexName === null) {
      indexName = await helper.getCurrentIndexName(collectionName);
    }

    let entries = [];
    if (isCollectionDraftPublish) {
      entries = await strapi.documents(collectionName).findMany({
        sort: { createdAt: 'DESC' },
        populate: populateAttrib['populate'],
        status: 'published',
      });
    } else {
      entries = await strapi.documents(collectionName).findMany({
        sort: { createdAt: 'DESC' },
        populate: populateAttrib['populate'],
      });
    }
    if (entries) {
      for (let s = 0; s < entries.length; s++) {
        const item = entries[s];
        const indexItemId = helper.getIndexItemId({
          collectionName: collectionName,
          itemDocumentId: item.documentId,
        });
        const collectionConfig = await configureIndexingService.getCollectionConfig({
          collectionName,
        });
        const dataToIndex = await helper.extractDataToIndex({
          collectionName,
          data: item,
          collectionConfig,
        });
        await esInterface.indexDataToSpecificIndex(
          { itemId: indexItemId, itemData: dataToIndex },
          indexName
        );
      }
    }
    return true;
  },
  async indexPendingData() {
    const scheduleIndexingService = strapi.plugins['elasticsearch'].services.scheduleIndexing;
    const configureIndexingService = strapi.plugins['elasticsearch'].services.configureIndexing;
    const logIndexingService = strapi.plugins['elasticsearch'].services.logIndexing;
    const esInterface = strapi.plugins['elasticsearch'].services.esInterface;
    const helper = strapi.plugins['elasticsearch'].services.helper;
    const recs = await scheduleIndexingService.getItemsPendingToBeIndexed();
    const fullSiteIndexing = recs.filter((r) => r.full_site_indexing === true).length > 0;
    if (fullSiteIndexing) {
      await this.rebuildIndex();
      for (let r = 0; r < recs.length; r++)
        await scheduleIndexingService.markIndexingTaskComplete(recs[r].documentId);
    } else {
      try {
        let fullCollectionIndexing = false;
        for (let r = 0; r < recs.length; r++) {
          const col = recs[r].collection_name;
          if (configureIndexingService.isCollectionConfiguredToBeIndexed(col)) {
            //Indexing the individual item
            if (recs[r].item_document_id) {
              if (recs[r].indexing_type !== 'remove-from-index') {
                const populateAttrib = helper.getPopulateForACollection({ collectionName: col });
                const item = await strapi.documents(col).findOne({
                  documentId: recs[r].item_document_id,
                  populate: populateAttrib['populate'],
                });
                const indexItemId = helper.getIndexItemId({
                  collectionName: col,
                  itemDocumentId: item.documentId,
                });
                const collectionConfig = await configureIndexingService.getCollectionConfig({
                  collectionName: col,
                });
                const dataToIndex = await helper.extractDataToIndex({
                  collectionName: col,
                  data: item,
                  collectionConfig,
                });
                await esInterface.indexData({
                  itemId: indexItemId,
                  itemData: dataToIndex,
                  collectionName: col,
                });
                await scheduleIndexingService.markIndexingTaskCompleteByItemDocumentId(
                  recs[r].item_document_id
                );
              } else {
                const indexItemId = helper.getIndexItemId({
                  collectionName: col,
                  itemDocumentId: recs[r].item_document_id,
                });
                await esInterface.removeItemFromIndex({ itemId: indexItemId, collectionName: col });
                await scheduleIndexingService.markIndexingTaskCompleteByItemDocumentId(
                  recs[r].item_document_id
                );
              }
            } //index the entire collection
            else {
              //PENDING : Index an entire collection
              await this.indexCollection(col);
              await scheduleIndexingService.markIndexingTaskComplete(recs[r].documentId);
              await logIndexingService.recordIndexingPass(
                'Indexing of collection ' + col + ' complete.'
              );
              fullCollectionIndexing = true;
            }
          } else await scheduleIndexingService.markIndexingTaskComplete(recs[r].documentId);
        }
        if (
          fullCollectionIndexing === false ||
          (fullCollectionIndexing === true && recs.length > 1)
        )
          await logIndexingService.recordIndexingPass(
            'Indexing of ' + String(recs.length) + ' records complete.'
          );
      } catch (err) {
        await logIndexingService.recordIndexingFail(
          'Indexing of records failed - ' + ' ' + String(err)
        );
        console.log(err);
        return false;
      }
    }
    return true;
  },
});
