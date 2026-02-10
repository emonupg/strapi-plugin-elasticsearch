const { Client } = require('@elastic/elasticsearch');
const fs = require('fs');
const path = require('path');

let client = null;

module.exports = ({ strapi }) => ({
  async initializeSearchEngine({ host, uname, password, cert }) {
    try {
      client = new Client({
        node: host,
        auth: {
          username: uname,
          password: password,
        },
        tls: {
          ca: cert,
          rejectUnauthorized: false,
        },
      });
    } catch (err) {
      if (err.message.includes('ECONNREFUSED')) {
        console.error(
          'strapi-plugin-elasticsearch : Connection to ElasticSearch at ',
          host,
          ' refused.'
        );
        console.error(err);
      } else {
        console.error(
          'strapi-plugin-elasticsearch : Error while initializing connection to ElasticSearch.'
        );
        console.error(err);
      }
      throw err;
    }
  },
  async createIndex(indexName) {
    try {
      const exists = await client.indices.exists({ index: indexName });
      if (!exists) {
        console.log(
          'strapi-plugin-elasticsearch : Search index ',
          indexName,
          ' does not exist. Creating index.'
        );

        await client.indices.create({
          index: indexName,
        });
      }
    } catch (err) {
      if (err.message.includes('ECONNREFUSED')) {
        console.log(
          'strapi-plugin-elasticsearch : Error while creating index - connection to ElasticSearch refused.'
        );
        console.log(err);
      } else {
        console.log('strapi-plugin-elasticsearch : Error while creating index.');
        console.log(err);
      }
    }
  },
  async deleteIndex(indexName) {
    try {
      const exists = await client.indices.exists({ index: indexName });
      if (!exists) {
        console.log(
          'strapi-plugin-elasticsearch : Index',
          indexName,
          'does not exist, skipping deletion.'
        );
        return;
      }
      await client.indices.delete({
        index: indexName,
      });
    } catch (err) {
      if (err.message.includes('ECONNREFUSED')) {
        console.log('strapi-plugin-elasticsearch : Connection to ElasticSearch refused.');
        console.log(err);
      } else if (err.meta?.body?.error?.type === 'index_not_found_exception') {
        console.log(
          'strapi-plugin-elasticsearch : Index',
          indexName,
          'not found, already deleted.'
        );
      } else {
        console.log('strapi-plugin-elasticsearch : Error while deleting index to ElasticSearch.');
        console.log(err);
      }
    }
  },
  async attachAliasToIndex(indexName) {
    try {
      const pluginConfig = await strapi.config.get('plugin::elasticsearch');
      const aliasName = pluginConfig.indexAliasName;
      const aliasExists = await client.indices.existsAlias({ name: aliasName });
      if (aliasExists) {
        console.log(
          'strapi-plugin-elasticsearch : Alias with this name already exists, removing it.'
        );
        await client.indices.deleteAlias({ index: '*', name: aliasName });
      }
      const indexExists = await client.indices.exists({ index: indexName });
      if (!indexExists) await this.createIndex(indexName);
      console.log(
        'strapi-plugin-elasticsearch : Attaching the alias ',
        aliasName,
        ' to index : ',
        indexName
      );
      await client.indices.putAlias({ index: indexName, name: aliasName });
    } catch (err) {
      if (err.message.includes('ECONNREFUSED')) {
        console.log(
          'strapi-plugin-elasticsearch : Attaching alias to the index - Connection to ElasticSearch refused.'
        );
        console.log(err);
      } else {
        console.log(
          'strapi-plugin-elasticsearch : Attaching alias to the index - Error while setting up alias within ElasticSearch.'
        );
        console.log(err);
      }
    }
  },
  async updateContentTypeAlias({ collectionName, newIndexName, oldIndexName }) {
    try {
      const helper = strapi.plugins['elasticsearch'].services.helper;
      const aliasName = helper.getIndexAlias(collectionName);

      const indexExists = await client.indices.exists({ index: newIndexName });
      if (!indexExists) {
        await this.createIndex(newIndexName);
      }

      const actions = [];

      if (oldIndexName) {
        const oldIndexExists = await client.indices.exists({ index: oldIndexName });
        if (oldIndexExists) {
          actions.push({
            remove: { index: oldIndexName, alias: aliasName },
          });
        }
      }

      actions.push({
        add: { index: newIndexName, alias: aliasName },
      });

      console.log(
        'strapi-plugin-elasticsearch : Updating alias ',
        aliasName,
        ' from ',
        oldIndexName,
        ' to ',
        newIndexName
      );
      await client.indices.updateAliases({ body: { actions } });

      return true;
    } catch (err) {
      console.log('strapi-plugin-elasticsearch : Error updating content-type alias');
      console.log(err);
      throw err;
    }
  },
  async updateGlobalSearchAlias() {
    try {
      const helper = strapi.plugins['elasticsearch'].services.helper;
      const globalAlias = helper.getGlobalSearchAlias();

      const settings = await helper.getStorageSettings();
      if (!settings || !settings.indexConfig) {
        console.log(
          'strapi-plugin-elasticsearch : No index configuration found for global alias update'
        );
        return false;
      }

      const indices = [];
      for (const collectionName of Object.keys(settings.indexConfig)) {
        const indexInfo = settings.indexConfig[collectionName];
        if (indexInfo && indexInfo.currentIndex) {
          const indexExists = await client.indices.exists({ index: indexInfo.currentIndex });
          if (indexExists) {
            indices.push(indexInfo.currentIndex);
          }
        }
      }

      if (indices.length === 0) {
        console.log('strapi-plugin-elasticsearch : No valid indices found for global alias');
        return false;
      }

      const aliasExists = await client.indices.existsAlias({ name: globalAlias });
      if (aliasExists) {
        await client.indices.deleteAlias({ index: '*', name: globalAlias });
      }

      const actions = indices.map((index) => ({
        add: { index, alias: globalAlias },
      }));

      console.log(
        'strapi-plugin-elasticsearch : Updating global search alias ',
        globalAlias,
        ' to point to ',
        indices.length,
        ' indices'
      );
      await client.indices.updateAliases({ body: { actions } });

      return true;
    } catch (err) {
      console.log('strapi-plugin-elasticsearch : Error updating global search alias');
      console.log(err);
      throw err;
    }
  },
  async checkESConnection() {
    if (!client) return false;
    try {
      await client.ping();
      return true;
    } catch (error) {
      console.error('strapi-plugin-elasticsearch : Could not connect to Elastic search.');
      console.error(error);
      return false;
    }
  },
  async indexDataToSpecificIndex({ itemId, itemData }, iName) {
    try {
      await client.index({
        index: iName,
        id: itemId,
        document: itemData,
      });
      await client.indices.refresh({ index: iName });
    } catch (err) {
      console.log(
        'strapi-plugin-elasticsearch : Error encountered while indexing data to ElasticSearch.'
      );
      console.log(err);
      throw err;
    }
  },
  async indexData({ itemId, itemData, collectionName }) {
    if (collectionName) {
      const helper = strapi.plugins['elasticsearch'].services.helper;
      const aliasName = helper.getIndexAlias(collectionName);
      return await this.indexDataToSpecificIndex({ itemId, itemData }, aliasName);
    } else {
      const pluginConfig = await strapi.config.get('plugin::elasticsearch');
      return await this.indexDataToSpecificIndex({ itemId, itemData }, pluginConfig.indexAliasName);
    }
  },
  async removeItemFromIndex({ itemId, collectionName }) {
    try {
      let indexTarget;
      if (collectionName) {
        const helper = strapi.plugins['elasticsearch'].services.helper;
        indexTarget = helper.getIndexAlias(collectionName);
      } else {
        const pluginConfig = await strapi.config.get('plugin::elasticsearch');
        indexTarget = pluginConfig.indexAliasName;
      }

      await client.delete({
        index: indexTarget,
        id: itemId,
      });
      await client.indices.refresh({ index: indexTarget });
    } catch (err) {
      if (err.meta.statusCode === 404)
        console.error(
          'strapi-plugin-elasticsearch : The entry to be removed from the index already does not exist.'
        );
      else {
        console.error(
          'strapi-plugin-elasticsearch : Error encountered while removing indexed data from ElasticSearch.'
        );
        throw err;
      }
    }
  },
  async searchData(searchQuery, collectionName = null) {
    try {
      let indexTarget;

      if (collectionName) {
        const helper = strapi.plugins['elasticsearch'].services.helper;
        indexTarget = helper.getIndexAlias(collectionName);
      } else {
        const helper = strapi.plugins['elasticsearch'].services.helper;
        const globalAlias = helper.getGlobalSearchAlias();

        const aliasExists = await client.indices.existsAlias({ name: globalAlias });

        if (aliasExists) {
          indexTarget = globalAlias;
        } else {
          const pluginConfig = await strapi.config.get('plugin::elasticsearch');
          indexTarget = pluginConfig.indexAliasName;
        }
      }

      const result = await client.search({
        index: indexTarget,
        ...searchQuery,
      });
      return result;
    } catch (err) {
      console.log(
        'Search : elasticClient.searchData : Error encountered while making a search request to ElasticSearch.'
      );
      console.log(err);
      throw err;
    }
  },
});
