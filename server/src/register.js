const stringifyForLog = (value) => {
  try {
    return JSON.stringify(value);
  } catch (err) {
    return JSON.stringify({ message: 'Unable to stringify log payload', error: err?.message });
  }
};

const register = ({ strapi }) => {
  strapi.documents.use(async (context, next) => {
    const result = await next();
    const scheduleIndexingService = strapi.plugins['elasticsearch'].services.scheduleIndexing;
    const configureIndexingService = strapi.plugins['elasticsearch'].services.configureIndexing;
    const indexer = strapi.plugins['elasticsearch'].services.indexer;
    const recordId = context.params?.documentId || result?.documentId;
    if (
      ['create', 'update', 'delete', 'publish', 'unpublish'].includes(context.action) &&
      strapi.elasticsearch.collections.includes(context.uid)
    ) {
      console.log(
        'strapi-plugin-elasticsearch : Document services trigger',
        stringifyForLog({
          action: context.action,
          collectionUid: context.uid,
          recordId,
          paramsData: context.params?.data || null,
          resultData: result || null,
        })
      );
      if (!recordId) {
        console.log(
          'strapi-plugin-elasticsearch : Document trigger skipped due to missing recordId',
          stringifyForLog({ action: context.action, collectionUid: context.uid })
        );
        return result;
      }

      let syncFilterField = null;
      let syncFilterValue = null;
      try {
        const collectionConfig = await configureIndexingService.getCollectionConfig({
          collectionName: context.uid,
        });
        syncFilterField = configureIndexingService.getSyncFilterField({
          collectionConfig,
          collectionName: context.uid,
        });
        if (syncFilterField) {
          const latestData =
            result && typeof result === 'object' ? result : context.params?.data || {};
          syncFilterValue = latestData[syncFilterField];
        }
      } catch (err) {
        console.log(
          'strapi-plugin-elasticsearch : Failed to evaluate sync filter field in document trigger',
          stringifyForLog({
            action: context.action,
            collectionUid: context.uid,
            recordId,
            error: err?.message,
          })
        );
      }

      let queueType = null;
      if (['delete', 'unpublish'].includes(context.action)) {
        queueType = 'remove';
      } else if (['create', 'update', 'publish'].includes(context.action)) {
        queueType = syncFilterField ? (syncFilterValue === true ? 'add' : 'remove') : 'add';
      }

      if (!queueType) {
        console.log(
          'strapi-plugin-elasticsearch : Document trigger ignored due to unsupported action',
          stringifyForLog({
            action: context.action,
            collectionUid: context.uid,
            recordId,
          })
        );
        return result;
      }

      console.log(
        'strapi-plugin-elasticsearch : Document trigger sync decision',
        stringifyForLog({
          action: context.action,
          collectionUid: context.uid,
          recordId,
          syncFilterField,
          syncFilterValue,
          queueType,
        })
      );

      try {
        const syncResult = await indexer.syncSingleItemNow({
          collectionName: context.uid,
          itemDocumentId: recordId,
          indexingType: queueType === 'remove' ? 'remove-from-index' : 'add-to-index',
          triggerAction: context.action,
        });
        console.log(
          'strapi-plugin-elasticsearch : Document trigger immediate sync success',
          stringifyForLog({
            action: context.action,
            collectionUid: context.uid,
            recordId,
            syncFilterField,
            syncFilterValue,
            queueType,
            syncResult,
          })
        );
      } catch (err) {
        console.log(
          'strapi-plugin-elasticsearch : Document trigger immediate sync failed, enqueue fallback task',
          stringifyForLog({
            action: context.action,
            collectionUid: context.uid,
            recordId,
            syncFilterField,
            syncFilterValue,
            queueType,
            error: err?.message,
          })
        );

        let queueTask = null;
        if (queueType === 'remove') {
          queueTask = await scheduleIndexingService.removeItemFromIndex({
            collectionUid: context.uid,
            recordId,
          });
        } else {
          queueTask = await scheduleIndexingService.addItemToIndex({
            collectionUid: context.uid,
            recordId,
          });
        }

        console.log(
          'strapi-plugin-elasticsearch : Document trigger enqueued fallback task',
          stringifyForLog({
            action: context.action,
            collectionUid: context.uid,
            recordId,
            syncFilterField,
            syncFilterValue,
            queueType,
            taskDocumentId: queueTask?.documentId,
          })
        );
      }
    }
    return result;
  });
};

module.exports = register;
