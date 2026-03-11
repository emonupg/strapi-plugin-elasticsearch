const stringifyForLog = (value) => {
  try {
    return JSON.stringify(value);
  } catch (err) {
    return JSON.stringify({ message: 'Unable to stringify log payload', error: err?.message });
  }
};

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
    // Full populate (including relations) — used in Phase 2 per-document findOne queries
    const populateAttrib = helper.getPopulateForACollection({ collectionName });
    const isCollectionDraftPublish = helper.isCollectionDraftPublish({ collectionName });
    const configureIndexingService = strapi.plugins['elasticsearch'].services.configureIndexing;
    const esInterface = strapi.plugins['elasticsearch'].services.esInterface;
    const idPageSize = 500;

    if (indexName === null) {
      indexName = await helper.getCurrentIndexName(collectionName);
    }

    const collectionConfig = await configureIndexingService.getCollectionConfig({ collectionName });
    const syncFilterField = configureIndexingService.getSyncFilterField({
      collectionConfig,
      collectionName,
    });

    // Lightweight query params for Phase 1 (ID fetching only)
    const idQueryParams = {
      sort: { createdAt: 'DESC' },
      fields: ['documentId'],
      populate: {},
    };

    if (isCollectionDraftPublish) {
      idQueryParams.status = 'published';
    }
    if (syncFilterField) {
      idQueryParams.filters = { [syncFilterField]: true };
    }

    console.log(
      'strapi-plugin-elasticsearch : indexCollection start',
      stringifyForLog({
        collectionName,
        indexName,
        idPageSize,
        isCollectionDraftPublish,
        syncFilterField,
      })
    );

    // Phase 1: Collect all documentIds using lightweight paginated queries (no populate)
    const allDocumentIds = [];
    let start = 0;
    while (true) {
      const idEntries = await strapi.documents(collectionName).findMany({
        ...idQueryParams,
        start,
        limit: idPageSize,
      });

      if (!idEntries || idEntries.length === 0) {
        break;
      }

      for (const entry of idEntries) {
        allDocumentIds.push(entry.documentId);
      }

      console.log(
        'strapi-plugin-elasticsearch : indexCollection IDs fetched',
        stringifyForLog({
          collectionName,
          start,
          fetched: idEntries.length,
          totalIds: allDocumentIds.length,
        })
      );

      start += idEntries.length;
      if (idEntries.length < idPageSize) {
        break;
      }
    }

    console.log(
      'strapi-plugin-elasticsearch : indexCollection Phase 1 complete',
      stringifyForLog({
        collectionName,
        indexName,
        totalDocumentIds: allDocumentIds.length,
      })
    );

    // Phase 2: Load each document with full populate via findOne and index it
    const findOneParams = {
      populate: populateAttrib['populate'],
    };
    if (isCollectionDraftPublish) {
      findOneParams.status = 'published';
    }

    let indexedItems = 0;
    for (const documentId of allDocumentIds) {
      try {
        const item = await strapi.documents(collectionName).findOne({
          documentId,
          ...findOneParams,
        });

        if (!item) {
          console.log(
            'strapi-plugin-elasticsearch : indexCollection item not found, skipping',
            stringifyForLog({ collectionName, documentId })
          );
          continue;
        }

        const indexItemId = helper.getIndexItemId({
          collectionName: collectionName,
          itemDocumentId: item.documentId,
        });
        const dataToIndex = await helper.extractDataToIndex({
          collectionName,
          data: item,
          collectionConfig,
        });
        await esInterface.indexDataToSpecificIndex(
          { itemId: indexItemId, itemData: dataToIndex },
          indexName,
          { refresh: false }
        );
        indexedItems += 1;

        if (indexedItems % 500 === 0) {
          console.log(
            'strapi-plugin-elasticsearch : indexCollection progress',
            stringifyForLog({
              collectionName,
              indexName,
              indexedItems,
              totalDocumentIds: allDocumentIds.length,
            })
          );
        }
      } catch (err) {
        console.error(
          'strapi-plugin-elasticsearch : indexCollection item error',
          stringifyForLog({ collectionName, documentId, error: err.message })
        );
      }
    }

    if (indexedItems > 0) {
      await esInterface.refreshIndex(indexName);
      console.log(
        'strapi-plugin-elasticsearch : indexCollection complete',
        stringifyForLog({ collectionName, indexName, indexedItems })
      );
    }

    return true;
  },
  async syncSingleItemNow({
    collectionName,
    itemDocumentId,
    indexingType = 'add-to-index',
    triggerAction = 'unknown',
  }) {
    const configureIndexingService = strapi.plugins['elasticsearch'].services.configureIndexing;
    const esInterface = strapi.plugins['elasticsearch'].services.esInterface;
    const helper = strapi.plugins['elasticsearch'].services.helper;

    if (!(await configureIndexingService.isCollectionConfiguredToBeIndexed({ collectionName }))) {
      console.log(
        'strapi-plugin-elasticsearch : syncSingleItemNow skipped, collection not configured',
        stringifyForLog({ collectionName, itemDocumentId, indexingType, triggerAction })
      );
      return { status: 'skipped-not-configured' };
    }

    const collectionConfig = await configureIndexingService.getCollectionConfig({ collectionName });
    const syncFilterField = configureIndexingService.getSyncFilterField({
      collectionConfig,
      collectionName,
    });
    const populateAttrib = helper.getPopulateForACollection({ collectionName });
    const isCollectionDraftPublish = helper.isCollectionDraftPublish({ collectionName });
    const indexItemId = helper.getIndexItemId({
      collectionName,
      itemDocumentId,
    });

    console.log(
      'strapi-plugin-elasticsearch : syncSingleItemNow start',
      stringifyForLog({
        collectionName,
        itemDocumentId,
        indexingType,
        triggerAction,
        syncFilterField,
        isCollectionDraftPublish,
      })
    );

    if (indexingType === 'remove-from-index') {
      await esInterface.removeItemFromIndex({ itemId: indexItemId, collectionName });
      console.log(
        'strapi-plugin-elasticsearch : syncSingleItemNow removed from ES due to explicit remove',
        stringifyForLog({ collectionName, itemDocumentId, indexItemId, triggerAction })
      );
      return { status: 'removed-explicit', indexItemId };
    }

    let item = null;
    if (isCollectionDraftPublish && syncFilterField) {
      item = await strapi.documents(collectionName).findOne({
        documentId: itemDocumentId,
        populate: populateAttrib['populate'],
      });
      console.log(
        'strapi-plugin-elasticsearch : syncSingleItemNow loaded draft item',
        stringifyForLog({
          collectionName,
          itemDocumentId,
          syncFilterField,
          syncFilterValue: item ? item[syncFilterField] : null,
        })
      );

      if (!item || item[syncFilterField] !== true) {
        await esInterface.removeItemFromIndex({ itemId: indexItemId, collectionName });
        console.log(
          'strapi-plugin-elasticsearch : syncSingleItemNow removed from ES due to draft filter decision',
          stringifyForLog({
            collectionName,
            itemDocumentId,
            indexItemId,
            syncFilterField,
            syncFilterValue: item ? item[syncFilterField] : null,
          })
        );
        return { status: 'removed-filter', indexItemId };
      }
    } else {
      const findOneParams = {
        documentId: itemDocumentId,
        populate: populateAttrib['populate'],
      };

      if (isCollectionDraftPublish) {
        findOneParams.status = 'published';
      }

      item = await strapi.documents(collectionName).findOne(findOneParams);
      console.log(
        'strapi-plugin-elasticsearch : syncSingleItemNow loaded item',
        stringifyForLog({
          collectionName,
          itemDocumentId,
          findOneParams,
          syncFilterField,
          syncFilterValue: syncFilterField && item ? item[syncFilterField] : null,
        })
      );

      if (!item || (syncFilterField && item[syncFilterField] !== true)) {
        await esInterface.removeItemFromIndex({ itemId: indexItemId, collectionName });
        console.log(
          'strapi-plugin-elasticsearch : syncSingleItemNow removed from ES due to item missing/filter',
          stringifyForLog({
            collectionName,
            itemDocumentId,
            indexItemId,
            syncFilterField,
            syncFilterValue: syncFilterField && item ? item[syncFilterField] : null,
          })
        );
        return { status: 'removed-filter-or-missing', indexItemId };
      }
    }

    const dataToIndex = await helper.extractDataToIndex({
      collectionName,
      data: item,
      collectionConfig,
    });
    await esInterface.indexData({
      itemId: indexItemId,
      itemData: dataToIndex,
      collectionName,
    });
    console.log(
      'strapi-plugin-elasticsearch : syncSingleItemNow indexed into ES',
      stringifyForLog({ collectionName, itemDocumentId, indexItemId, indexedData: dataToIndex })
    );

    return { status: 'indexed', indexItemId };
  },
  async indexPendingData() {
    const scheduleIndexingService = strapi.plugins['elasticsearch'].services.scheduleIndexing;
    const configureIndexingService = strapi.plugins['elasticsearch'].services.configureIndexing;
    const logIndexingService = strapi.plugins['elasticsearch'].services.logIndexing;
    const recs = await scheduleIndexingService.getItemsPendingToBeIndexed();
    console.log(
      'strapi-plugin-elasticsearch : indexPendingData start',
      stringifyForLog({
        recCount: recs.length,
        records: recs.map((rec) => ({
          taskDocumentId: rec.documentId,
          collection_name: rec.collection_name,
          item_document_id: rec.item_document_id,
          indexing_type: rec.indexing_type,
          full_site_indexing: rec.full_site_indexing,
          createdAt: rec.createdAt,
        })),
      })
    );
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
          console.log(
            'strapi-plugin-elasticsearch : indexPendingData processing task',
            stringifyForLog({
              taskDocumentId: recs[r].documentId,
              collection_name: col,
              item_document_id: recs[r].item_document_id,
              indexing_type: recs[r].indexing_type,
              createdAt: recs[r].createdAt,
            })
          );
          if (
            await configureIndexingService.isCollectionConfiguredToBeIndexed({
              collectionName: col,
            })
          ) {
            //Indexing the individual item
            if (recs[r].item_document_id) {
              const itemDocumentId = recs[r].item_document_id;
              const processedTaskCreatedAt = recs[r].createdAt;
              const syncResult = await this.syncSingleItemNow({
                collectionName: col,
                itemDocumentId,
                indexingType: recs[r].indexing_type,
                triggerAction: 'queued-task',
              });
              console.log(
                'strapi-plugin-elasticsearch : indexPendingData single-item sync result',
                stringifyForLog({
                  taskDocumentId: recs[r].documentId,
                  collection_name: col,
                  item_document_id: itemDocumentId,
                  indexing_type: recs[r].indexing_type,
                  syncResult,
                })
              );
              await scheduleIndexingService.markIndexingTaskCompleteByItemDocumentId({
                recId: itemDocumentId,
                collectionUid: col,
                processedTaskCreatedAt,
              });
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
