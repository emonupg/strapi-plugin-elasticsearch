///START : via https://raw.githubusercontent.com/Barelydead/strapi-plugin-populate-deep/main/server/helpers/index.js

const { isEmpty, merge } = require('lodash/fp');
const transformServiceProvider = require('./transform-content');

const getPluginStore = () => {
  return strapi.store({
    environment: '',
    type: 'plugin',
    name: 'elasticsearch',
  });
};

const getModelPopulationAttributes = (model) => {
  if (model.uid === 'plugin::upload.file') {
    const { related, ...attributes } = model.attributes;
    return attributes;
  }

  return model.attributes;
};

const getFullPopulateObject = (modelUid, maxDepth = 20, ignore) => {
  const skipCreatorFields = true;

  if (maxDepth <= 1) {
    return true;
  }
  if (modelUid === 'admin::user' && skipCreatorFields) {
    return undefined;
  }

  const populate = {};
  const model = strapi.getModel(modelUid);
  if (ignore && !ignore.includes(model.collectionName)) ignore.push(model.collectionName);
  for (const [key, value] of Object.entries(getModelPopulationAttributes(model))) {
    if (ignore?.includes(key)) continue;
    if (value) {
      if (value.type === 'component') {
        populate[key] = getFullPopulateObject(value.component, maxDepth - 1);
      } else if (value.type === 'dynamiczone') {
        const dynamicPopulate = value.components.reduce((prev, cur) => {
          const curPopulate = getFullPopulateObject(cur, maxDepth - 1);
          return curPopulate === true ? prev : merge(prev, curPopulate);
        }, {});

        populate[key] = isEmpty(dynamicPopulate) ? true : { on: dynamicPopulate.populate };
      } else if (value.type === 'relation') {
        const relationPopulate = getFullPopulateObject(
          value.target,
          key === 'localizations' && maxDepth > 2 ? 1 : maxDepth - 1,
          ignore
        );
        if (relationPopulate) {
          populate[key] = relationPopulate;
        }
      } else if (value.type === 'media') {
        populate[key] = true;
      }
    }
  }
  return isEmpty(populate) ? true : { populate };
};

///END : via https://raw.githubusercontent.com/Barelydead/strapi-plugin-populate-deep/main/server/helpers/index.js

const getPopulateObjectForComponent = (componentUid) => {
  const componentSchema = strapi
    .plugin('content-manager')
    .service('components')
    .findAllComponents()
    .filter((c) => c.uid === componentUid)[0];
  const componentAttributes = componentSchema.attributes;
  const populate = {};
  for (const attributeName of Object.keys(componentAttributes)) {
    const attribute = componentAttributes[attributeName];
    if (attribute.type === 'component') {
      populate[attributeName] = getPopulateObjectForComponent(attribute.component);
    } else if (attribute.type === 'media') {
      populate[attributeName] = { fields: ['*'] };
    }
  }
  return { populate };
};

const getPopulateForACollection = (collectionUid) => {
  const collection = strapi
    .plugin('content-manager')
    .service('content-types')
    .findAllContentTypes()
    .filter((c) => c.uid === collectionUid)[0];

  if (!collection) {
    console.log(
      `strapi-plugin-elasticsearch : Collection ${collectionUid} not found in content-manager`
    );
    return { populate: {}, fields: [] };
  }

  const selCollAttributes = collection.attributes;
  const populate = {};
  const fields = [];
  for (const attributeName of Object.keys(selCollAttributes)) {
    const attribute = selCollAttributes[attributeName];
    if (attribute.type === 'dynamiczone') {
      populate[attributeName] = {
        on: attribute.components.reduce((acc, componentUid) => {
          acc[componentUid] = getPopulateObjectForComponent(componentUid);
          return acc;
        }, {}),
      };
    } else if (attribute.type === 'component') {
      populate[attributeName] = getPopulateObjectForComponent(attribute.component);
    } else if (attribute.type === 'media') {
      populate[attributeName] = { fields: ['*'] };
    } else if (attribute.type === 'relation') {
      populate[attributeName] = { fields: ['*'] };
    } else {
      fields.push(attributeName);
    }
  }
  return { populate, fields };
};

/*
//Example config to cover extraction cases
            collectionConfig[collectionName] = {
                'major' : {index: true},
                'sections' : { index: true, searchFieldName: 'information',
                    'subfields' : [
                        { 'component' : 'try.paragraph',
                            'field' : 'Text'},
                        { 'component' : 'try.paragraph',
                            'field' : 'Heading'},
                        { 'component' : 'try.footer',
                            'field' : 'footer_link',
                            'subfields' :[ {
                                'component' : 'try.link',
                                'field' : 'display_text'
                            }]
                        }] },
                'seo_details' : {
                    index: true, searchFieldName: 'seo',
                    'subfields' : [
                        {
                            'component' : 'try.seo',
                            'field' : 'meta_description'
                        }
                    ]
                },
                'changelog' : {
                    index: true, searchFieldName: 'breakdown',
                    'subfields' : [
                        {
                            'component' : 'try.revision',
                            'field' : 'summary'
                        }
                    ]
                }
            }
*/
function extractSubfieldData({ config, data }) {
  let returnData = '';
  if (data === null) return returnData;
  if (Array.isArray(data)) {
    const dynDataItems = data;
    for (let r = 0; r < dynDataItems.length; r++) {
      const extractItem = dynDataItems[r];
      for (let s = 0; s < config.length; s++) {
        const conf = config[s];
        if (Object.keys(extractItem).includes('__component')) {
          if (
            conf.component === extractItem.__component &&
            !Object.keys(conf).includes('subfields') &&
            typeof extractItem[conf['field']] !== 'undefined' &&
            extractItem[conf['field']]
          ) {
            let val = extractItem[conf['field']];
            if (Object.keys(conf).includes('transform') && conf['transform'] === 'markdown')
              val = transformServiceProvider.transform({ content: val, from: 'markdown' });
            returnData = returnData + '\n' + val;
          } else if (
            conf.component === extractItem.__component &&
            Object.keys(conf).includes('subfields')
          ) {
            returnData =
              returnData +
              '\n' +
              extractSubfieldData({
                config: conf['subfields'],
                data: extractItem[conf['field']],
              });
          }
        } else {
          if (
            !Object.keys(conf).includes('subfields') &&
            typeof extractItem[conf['field']] !== 'undefined' &&
            extractItem[conf['field']]
          ) {
            let val = extractItem[conf['field']];
            if (Object.keys(conf).includes('transform') && conf['transform'] === 'markdown')
              val = transformServiceProvider.transform({ content: val, from: 'markdown' });
            returnData = returnData + '\n' + val;
          } else if (Object.keys(conf).includes('subfields')) {
            returnData =
              returnData +
              '\n' +
              extractSubfieldData({
                config: conf['subfields'],
                data: extractItem[conf['field']],
              });
          }
        }
      }
    }
  } //for single component as a field
  else {
    for (let s = 0; s < config.length; s++) {
      const conf = config[s];
      if (
        !Object.keys(conf).includes('subfields') &&
        typeof data[conf['field']] !== 'undefined' &&
        data[conf['field']]
      )
        returnData = returnData + '\n' + data[conf['field']];
      else if (Object.keys(conf).includes('subfields')) {
        returnData =
          returnData +
          '\n' +
          extractSubfieldData({
            config: conf['subfields'],
            data: data[conf['field']],
          });
      }
    }
  }
  return returnData;
}

const tranformValueBeforeSubmittingToElasticsearch = (val, transformerFunctionName) => {
  const transformerFunctionsList = strapi.plugins['elasticsearch'].config('transformers');
  if (Object.keys(transformerFunctionsList).includes(transformerFunctionName)) {
    const transformerFunction = transformerFunctionsList[transformerFunctionName];
    return transformerFunction(val);
  } else return val;
};

module.exports = ({ strapi }) => ({
  async getElasticsearchInfo() {
    const configureService = strapi.plugins['elasticsearch'].services.configureIndexing;
    const esInterface = strapi.plugins['elasticsearch'].services.esInterface;
    const pluginConfig = await strapi.config.get('plugin::elasticsearch');

    const connected =
      pluginConfig.searchConnector && pluginConfig.searchConnector.host
        ? await esInterface.checkESConnection()
        : false;

    return {
      indexingCronSchedule: pluginConfig.indexingCronSchedule || 'Not configured',
      elasticHost: pluginConfig.searchConnector
        ? pluginConfig.searchConnector.host || 'Not configured'
        : 'Not configured',
      elasticUserName: pluginConfig.searchConnector
        ? pluginConfig.searchConnector.username || 'Not configured'
        : 'Not configured',
      elasticCertificate: pluginConfig.searchConnector
        ? pluginConfig.searchConnector.certificate || 'Not configured'
        : 'Not configured',
      elasticIndexAlias: pluginConfig.indexAliasName || 'Not configured',
      connected: connected,
      initialized: configureService.isInitialized(),
    };
  },
  isCollectionDraftPublish({ collectionName }) {
    const model = strapi.getModel(collectionName);
    if (!model) {
      console.log(`strapi-plugin-elasticsearch : Model ${collectionName} not found`);
      return false;
    }
    return model.attributes.publishedAt ? true : false;
  },
  getPopulateAttribute({ collectionName }) {
    //TODO : We currently have set populate to upto 4 levels, should
    //this be configurable or a different default value?
    return getFullPopulateObject(collectionName, 4, []);
  },
  getPopulateForACollection({ collectionName }) {
    return getPopulateForACollection(collectionName);
  },
  getIndexItemId({ collectionName, itemDocumentId }) {
    // For per-content-type indices, we only use the document ID
    // The collection type is already separated by having different indices
    return itemDocumentId;
  },
  async getStorageSettings() {
    const pluginStore = getPluginStore();
    const settings = await pluginStore.get({ key: 'configsettings' });
    if (settings) {
      return JSON.parse(settings);
    }
    return null;
  },
  async getCurrentIndexName(collectionName) {
    if (!collectionName) {
      throw new Error('collectionName is required for getCurrentIndexName');
    }
    const settings = await this.getStorageSettings();
    if (settings && settings.indexConfig && settings.indexConfig[collectionName]) {
      return settings.indexConfig[collectionName].currentIndex;
    }
    const shortName = collectionName.replace(/^api::/, '').replace(/\./g, '-');
    return `strapi-plugin-${shortName}-index_001`;
  },
  async getIncrementedIndexName(collectionName) {
    if (!collectionName) {
      throw new Error('collectionName is required for getIncrementedIndexName');
    }
    const currentIndexName = await this.getCurrentIndexName(collectionName);
    const match = currentIndexName.match(/_(\d+)$/);
    if (match) {
      const version = parseInt(match[1]) + 1;
      const baseName = currentIndexName.substring(0, currentIndexName.lastIndexOf('_'));
      return `${baseName}_${String(version).padStart(3, '0')}`;
    }
    return `${currentIndexName}_002`;
  },
  getIndexAlias(collectionName) {
    if (!collectionName) {
      throw new Error('collectionName is required for getIndexAlias');
    }
    const shortName = collectionName.replace(/^api::/, '').replace(/\./g, '-');
    return `strapi-alias-${shortName}`;
  },
  getGlobalSearchAlias() {
    return 'strapi-search-all';
  },
  async storeIndexInfo({ collectionName, indexName, version }) {
    if (!collectionName || !indexName) {
      throw new Error('collectionName and indexName are required for storeIndexInfo');
    }
    const pluginStore = getPluginStore();
    const settings = (await this.getStorageSettings()) || {};

    // Initialize indexConfig if it doesn't exist
    if (!settings.indexConfig) {
      settings.indexConfig = {};
    }

    // Store per-content-type index information
    settings.indexConfig[collectionName] = {
      currentIndex: indexName,
      version: version || 1,
      alias: this.getIndexAlias(collectionName),
      lastRebuilt: new Date().toISOString(),
    };

    await pluginStore.set({ key: 'configsettings', value: JSON.stringify(settings) });
  },
  async storeCurrentIndexName(indexName) {
    // Legacy method - kept for backward compatibility
    // New code should use storeIndexInfo instead
    const pluginStore = getPluginStore();
    const settings = (await this.getStorageSettings()) || {};
    settings['indexConfig'] = { name: indexName };
    await pluginStore.set({ key: 'configsettings', value: JSON.stringify(settings) });
  },
  modifySubfieldsConfigForExtractor(collectionConfig) {
    const collectionName = Object.keys(collectionConfig)[0];
    const attributes = Object.keys(collectionConfig[collectionName]);
    for (let r = 0; r < attributes.length; r++) {
      const attr = attributes[r];
      const attribFields = Object.keys(collectionConfig[collectionName][attr]);
      if (attribFields.includes('subfields')) {
        const subfielddata = collectionConfig[collectionName][attr]['subfields'];
        if (subfielddata.length > 0) {
          try {
            const subfieldjson = JSON.parse(subfielddata);
            if (Object.keys(subfieldjson).includes('subfields'))
              collectionConfig[collectionName][attr]['subfields'] = subfieldjson['subfields'];
          } catch (err) {
            continue;
          }
        }
      }
    }
    return collectionConfig;
  },
  extractDataToIndex({ collectionName, data, collectionConfig }) {
    collectionConfig = this.modifySubfieldsConfigForExtractor(collectionConfig);
    const fti = Object.keys(collectionConfig[collectionName]);
    const document = {};
    for (let k = 0; k < fti.length; k++) {
      const fieldConfig = collectionConfig[collectionName][fti[k]];
      if (fieldConfig.index) {
        let val = null;
        if (Object.keys(fieldConfig).includes('subfields')) {
          val = extractSubfieldData({ config: fieldConfig['subfields'], data: data[fti[k]] });
          val = val ? val.trim() : val;
        } else {
          val = data[fti[k]];
          if (
            Object.keys(fieldConfig).includes('transform') &&
            fieldConfig['transform'] === 'markdown'
          )
            val = transformServiceProvider.transform({ content: val, from: 'markdown' });
        }

        if (Object.keys(fieldConfig).includes('searchFieldName'))
          document[fieldConfig['searchFieldName']] = fieldConfig['transformerFunction']
            ? tranformValueBeforeSubmittingToElasticsearch(val, fieldConfig['transformerFunction'])
            : val;
        else
          document[fti[k]] = fieldConfig['transformerFunction']
            ? tranformValueBeforeSubmittingToElasticsearch(val, fieldConfig['transformerFunction'])
            : val;
      }
    }
    return document;
  },
});
