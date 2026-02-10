'use strict';

const { Client } = require('@elastic/elasticsearch');

module.exports = ({ strapi }) => ({
  async validateRebuild({ collectionName, indexName }) {
    try {
      const helper = strapi.plugins['elasticsearch'].services.helper;
      const configureIndexingService = strapi.plugins['elasticsearch'].services.configureIndexing;
      const pluginConfig = await strapi.config.get('plugin::elasticsearch');

      const client = new Client({
        node: pluginConfig.searchConnector.host,
        auth: {
          username: pluginConfig.searchConnector.username,
          password: pluginConfig.searchConnector.password,
        },
        tls: {
          ca: pluginConfig.searchConnector.certificate,
          rejectUnauthorized: false,
        },
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
      const populateAttrib = helper.getPopulateForACollection({ collectionName });

      let strapiEntries = [];
      if (isCollectionDraftPublish) {
        strapiEntries = await strapi.documents(collectionName).findMany({
          sort: { createdAt: 'DESC' },
          populate: populateAttrib['populate'],
          status: 'published',
        });
      } else {
        strapiEntries = await strapi.documents(collectionName).findMany({
          sort: { createdAt: 'DESC' },
          populate: populateAttrib['populate'],
        });
      }

      const strapiCount = strapiEntries.length;

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

      if (strapiEntries.length > 0) {
        const sampleSize = Math.min(5, strapiEntries.length);
        const sampleIndices = [];
        for (let i = 0; i < sampleSize; i++) {
          const randomIndex = Math.floor(Math.random() * strapiEntries.length);
          if (!sampleIndices.includes(randomIndex)) {
            sampleIndices.push(randomIndex);
          }
        }

        for (const idx of sampleIndices) {
          const sampleEntry = strapiEntries[idx];
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
