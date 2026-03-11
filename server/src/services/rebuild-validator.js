'use strict';

const { Client } = require('@elastic/elasticsearch');

module.exports = ({ strapi }) => ({
  async validateRebuild({ collectionName, indexName }) {
    try {
      const helper = strapi.plugins['elasticsearch'].services.helper;
      const configureIndexingService = strapi.plugins['elasticsearch'].services.configureIndexing;
      const pluginConfig = await strapi.config.get('plugin::elasticsearch');
      const connector = pluginConfig.searchConnector || {};
      const requestTimeout =
        Number.isFinite(Number(connector.requestTimeout)) && Number(connector.requestTimeout) > 0
          ? Number(connector.requestTimeout)
          : 120000;
      const maxRetries =
        Number.isFinite(Number(connector.maxRetries)) && Number(connector.maxRetries) >= 0
          ? Number(connector.maxRetries)
          : 3;

      const client = new Client({
        node: connector.host,
        auth: {
          username: connector.username,
          password: connector.password,
        },
        tls: {
          ca: connector.certificate,
          rejectUnauthorized: false,
        },
        requestTimeout,
        maxRetries,
      });

      const validationResult = {
        success: true,
        collectionName,
        indexName,
        checks: [],
      };

      const indexExists = await client.indices.exists({ index: indexName });
      if (!indexExists) {
        validationResult.success = false;
        validationResult.checks.push({
          type: 'index_existence',
          passed: false,
          message: `Index ${indexName} does not exist`,
        });
        return validationResult;
      }

      validationResult.checks.push({
        type: 'index_existence',
        passed: true,
        message: `Index ${indexName} exists`,
      });

      const isCollectionDraftPublish = helper.isCollectionDraftPublish({ collectionName });
      // Use no-relations populate to avoid PostgreSQL bind-parameter overflow
      const populateAttrib = helper.getPopulateForACollectionNoRelations({ collectionName });
      const collectionConfig = await configureIndexingService.getCollectionConfig({
        collectionName,
      });
      const syncFilterField = configureIndexingService.getSyncFilterField({
        collectionConfig,
        collectionName,
      });

      const queryParams = {
        sort: { createdAt: 'DESC' },
        populate: populateAttrib['populate'],
      };

      if (isCollectionDraftPublish) {
        queryParams.status = 'published';
      }
      if (syncFilterField) {
        queryParams.filters = { [syncFilterField]: true };
      }

      // Count Strapi records using pagination to avoid bind-parameter overflow
      const countPageSize = 100;
      let strapiCount = 0;
      let countStart = 0;
      while (true) {
        const page = await strapi.documents(collectionName).findMany({
          ...queryParams,
          start: countStart,
          limit: countPageSize,
          fields: ['documentId'],
          populate: {},
        });
        if (!page || page.length === 0) break;
        strapiCount += page.length;
        countStart += page.length;
        if (page.length < countPageSize) break;
      }

      const esCountResult = await client.count({ index: indexName });
      const esCount = esCountResult.count;

      const countMatch = strapiCount === esCount;
      validationResult.checks.push({
        type: 'document_count',
        passed: countMatch,
        message: countMatch
          ? `Document count matches: ${strapiCount} documents`
          : `Document count mismatch: Strapi has ${strapiCount}, Elasticsearch has ${esCount}`,
        strapiCount,
        esCount,
      });

      if (!countMatch) {
        validationResult.success = false;
      }

      // Sample random documents for spot-checking
      if (strapiCount > 0) {
        const sampleSize = Math.min(5, strapiCount);
        const sampleOffsets = new Set();
        while (sampleOffsets.size < sampleSize) {
          sampleOffsets.add(Math.floor(Math.random() * strapiCount));
        }

        for (const offset of sampleOffsets) {
          const samplePage = await strapi.documents(collectionName).findMany({
            ...queryParams,
            start: offset,
            limit: 1,
            fields: ['documentId'],
            populate: {},
          });

          if (!samplePage || samplePage.length === 0) continue;

          const sampleEntry = samplePage[0];
          const docId = helper.getIndexItemId({
            collectionName,
            itemDocumentId: sampleEntry.documentId,
          });

          try {
            const esDoc = await client.get({
              index: indexName,
              id: docId,
            });

            validationResult.checks.push({
              type: 'sample_document',
              passed: true,
              message: `Sample document ${docId} exists in index`,
              documentId: docId,
            });
          } catch (err) {
            if (err.meta?.statusCode === 404) {
              validationResult.success = false;
              validationResult.checks.push({
                type: 'sample_document',
                passed: false,
                message: `Sample document ${docId} not found in index`,
                documentId: docId,
              });
            } else {
              throw err;
            }
          }
        }
      }

      return validationResult;
    } catch (err) {
      console.error('strapi-plugin-elasticsearch : Error validating rebuild');
      console.error(err);
      return {
        success: false,
        collectionName,
        indexName,
        error: err.message,
        checks: [],
      };
    }
  },
});
