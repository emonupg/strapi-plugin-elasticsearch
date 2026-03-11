const stringifyForLog = (value) => {
  try {
    return JSON.stringify(value);
  } catch (err) {
    return JSON.stringify({ message: 'Unable to stringify log payload', error: err?.message });
  }
};

module.exports = ({ strapi }) => ({
  async addFullSiteIndexingTask() {
    const data = await strapi.documents('plugin::elasticsearch.task').create({
      data: {
        collection_name: '',
        indexing_status: 'to-be-done',
        full_site_indexing: true,
        indexing_type: 'add-to-index',
      },
    });
    console.log(
      'strapi-plugin-elasticsearch : Queue task created',
      stringifyForLog({
        queueAction: 'addFullSiteIndexingTask',
        taskDocumentId: data?.documentId,
        full_site_indexing: data?.full_site_indexing,
        indexing_type: data?.indexing_type,
      })
    );
    return data;
  },
  async addCollectionToIndex({ collectionUid }) {
    const data = await strapi.documents('plugin::elasticsearch.task').create({
      data: {
        collection_name: collectionUid,
        indexing_status: 'to-be-done',
        full_site_indexing: false,
        indexing_type: 'add-to-index',
      },
    });
    console.log(
      'strapi-plugin-elasticsearch : Queue task created',
      stringifyForLog({
        queueAction: 'addCollectionToIndex',
        taskDocumentId: data?.documentId,
        collection_name: collectionUid,
        indexing_type: data?.indexing_type,
      })
    );
    return data;
  },
  async addItemToIndex({ collectionUid, recordId }) {
    const data = await strapi.documents('plugin::elasticsearch.task').create({
      data: {
        item_document_id: recordId,
        collection_name: collectionUid,
        indexing_status: 'to-be-done',
        full_site_indexing: false,
        indexing_type: 'add-to-index',
      },
    });
    console.log(
      'strapi-plugin-elasticsearch : Queue task created',
      stringifyForLog({
        queueAction: 'addItemToIndex',
        taskDocumentId: data?.documentId,
        collection_name: collectionUid,
        item_document_id: recordId,
        indexing_type: data?.indexing_type,
      })
    );
    return data;
  },
  async removeItemFromIndex({ collectionUid, recordId }) {
    const data = await strapi.documents('plugin::elasticsearch.task').create({
      data: {
        item_document_id: recordId,
        collection_name: collectionUid,
        indexing_status: 'to-be-done',
        full_site_indexing: false,
        indexing_type: 'remove-from-index',
      },
    });
    console.log(
      'strapi-plugin-elasticsearch : Queue task created',
      stringifyForLog({
        queueAction: 'removeItemFromIndex',
        taskDocumentId: data?.documentId,
        collection_name: collectionUid,
        item_document_id: recordId,
        indexing_type: data?.indexing_type,
      })
    );
    return data;
  },
  async getItemsPendingToBeIndexed() {
    const entries = await strapi.documents('plugin::elasticsearch.task').findMany({
      filters: { indexing_status: 'to-be-done' },
      sort: { createdAt: 'DESC' },
    });
    console.log(
      'strapi-plugin-elasticsearch : Queue fetch pending tasks',
      stringifyForLog({
        count: entries.length,
        tasks: entries.map((entry) => ({
          taskDocumentId: entry.documentId,
          collection_name: entry.collection_name,
          item_document_id: entry.item_document_id,
          full_site_indexing: entry.full_site_indexing,
          indexing_type: entry.indexing_type,
          createdAt: entry.createdAt,
        })),
      })
    );
    return entries;
  },
  async markIndexingTaskComplete(recId) {
    await strapi.documents('plugin::elasticsearch.task').update({
      documentId: recId,
      data: {
        indexing_status: 'done',
      },
    });
    console.log(
      'strapi-plugin-elasticsearch : Queue task completed by task id',
      stringifyForLog({ taskDocumentId: recId })
    );
  },
  async markIndexingTaskCompleteByItemDocumentId({ recId, collectionUid, processedTaskCreatedAt }) {
    const filters = {
      item_document_id: recId,
      indexing_status: 'to-be-done',
    };
    if (collectionUid) {
      filters.collection_name = collectionUid;
    }
    if (processedTaskCreatedAt) {
      filters.createdAt = { $lte: processedTaskCreatedAt };
    }
    const itemsToUpdate = await strapi.documents('plugin::elasticsearch.task').findMany({
      filters,
    });
    console.log(
      'strapi-plugin-elasticsearch : Queue complete-by-item candidate tasks',
      stringifyForLog({
        filters,
        matchedTaskDocumentIds: itemsToUpdate.map((item) => item.documentId),
      })
    );
    if (itemsToUpdate.length > 0) {
      for (let k = 0; k < itemsToUpdate.length; k++) {
        await strapi.documents('plugin::elasticsearch.task').update({
          documentId: itemsToUpdate[k].documentId,
          data: {
            indexing_status: 'done',
          },
        });
        console.log(
          'strapi-plugin-elasticsearch : Queue task completed by item id',
          stringifyForLog({
            taskDocumentId: itemsToUpdate[k].documentId,
            item_document_id: recId,
            collection_name: collectionUid,
          })
        );
      }
    }
  },
});
