import React, { useState, useEffect } from 'react';
import { Page } from '@strapi/admin/strapi-admin';
import { useFetchClient } from '@strapi/admin/strapi-admin';
import { useParams } from 'react-router-dom';
import { SubNavigation } from '../components/SubNavigation';
import {
  Box,
  Flex,
  SingleSelect,
  SingleSelectOption,
  Toggle,
  Link,
  Alert,
  Button,
  Typography,
  Textarea,
  TextInput,
  Modal,
} from '@strapi/design-system';
import { ArrowLeft } from '@strapi/icons';
import pluginId from '../pluginId';
import {
  apiGetCollectionConfig,
  apiSaveCollectionConfig,
  apiGetTransformers,
} from '../utils/apiUrls';
import Loader from '../components/Loader';

const ConfigureField = ({ config, index, setFieldConfig, transformersList }) => {
  const validateSubfieldsConfig = (conf) => {
    if (conf && conf.length > 0) {
      try {
        JSON.parse(conf);
        return true;
      } catch (e) {
        return false;
      }
    } else return true;
  };

  const updateIndex = (checked) => {
    setFieldConfig({ index, config: { ...config, index: checked } });
  };

  const updateSubfieldConfig = (subfields) => {
    const subfieldsConfigValid = validateSubfieldsConfig(subfields);
    setFieldConfig({ index, config: { ...config, subfields, subfieldsConfigValid } });
  };

  const updateMappedFieldName = (mappedName) => {
    setFieldConfig({ index, config: { ...config, searchFieldName: mappedName } });
  };

  const updateTransformerFunction = (transformerFunction) => {
    setFieldConfig({ index, config: { ...config, transformerFunction } });
  };

  return (
    <Box background="neutral100" borderColor="neutral200" hasRadius index={index} padding={4}>
      <Box paddingTop={2} paddingBottom={2}>
        <Typography fontWeight="bold" textColor="neutral600">
          {config.name}
        </Typography>
      </Box>
      <Box paddingTop={2} paddingBottom={2}>
        <Toggle
          label="Index"
          onLabel="Yes"
          offLabel="No"
          checked={config.index}
          onChange={(e) => updateIndex(e.target.checked)}
        />
      </Box>
      <Flex direction="row" gap={2}>
        <Box width="50%" paddingTop={2} paddingBottom={2}>
          <TextInput
            label="Maps to search field"
            placeholder="Enter field name"
            name="Search field"
            onChange={(e) => updateMappedFieldName(e.target.value)}
            value={config.searchFieldName || ''}
          />
        </Box>
        {transformersList.length > 0 && (
          <Box width="50%" paddingTop={2} paddingBottom={2}>
            <SingleSelect
              placeholder="Select a transformer function"
              name="Transformer function"
              value={config.transformerFunction || ''}
              onChange={updateTransformerFunction}
            >
              <SingleSelectOption value="">None</SingleSelectOption>
              {transformersList.map((transformer) => (
                <SingleSelectOption value={transformer} key={transformer}>
                  {transformer}
                </SingleSelectOption>
              ))}
            </SingleSelect>
          </Box>
        )}
      </Flex>
      {config.index && config.type && config.type === 'dynamiczone' ? (
        <Box paddingTop={2} paddingBottom={2}>
          <Textarea
            label="Dynamic zone fields to index"
            error={
              config.subfieldsConfigValid === false ? 'Invalid indexing configuration' : undefined
            }
            onChange={(e) => updateSubfieldConfig(e.target.value)}
          >
            {config.subfields || ''}
          </Textarea>
        </Box>
      ) : null}
      {config.index && config.type && config.type === 'component' ? (
        <Box paddingTop={2} paddingBottom={2}>
          <Textarea
            label="Component fields to index"
            error={
              config.subfieldsConfigValid === false ? 'Invalid indexing configuration' : undefined
            }
            onChange={(e) => updateSubfieldConfig(e.target.value)}
          >
            {config.subfields || ''}
          </Textarea>
        </Box>
      ) : null}
    </Box>
  );
};

const ConfigureCollection = () => {
  const [isInProgress, setIsInProgress] = useState(false);
  const [selectedCollection, setSelectedCollection] = useState(null);
  const [collectionConfig, setCollectionConfig] = useState(null);
  const [transformersList, setTransformersList] = useState([]);
  const params = useParams();
  const [alertContent, setAlertContent] = useState(null);
  const { get, post } = useFetchClient();
  const showMessage = ({ variant, title, text }) => {
    setAlertContent({ variant, title, text });
    setTimeout(() => {
      setAlertContent(null);
    }, 5000);
  };
  const updateCollectionsConfig = ({ index, config }) => {
    setCollectionConfig({
      collectionName: collectionConfig.collectionName,
      attributes: collectionConfig.attributes.map((e, idx) => (index === idx ? config : e)),
    });
  };

  const loadTransformers = () => {
    return get(apiGetTransformers).then((resp) => resp.data);
  };

  const loadConfigForCollection = (collectionName) => {
    return get(apiGetCollectionConfig(collectionName)).then((resp) => resp.data);
  };

  const saveConfigForCollection = (collectionName, data) => {
    return post(apiSaveCollectionConfig(collectionName), {
      data,
    });
  };

  const saveCollectionConfig = () => {
    if (collectionConfig && collectionConfig.collectionName) {
      const data = {};
      data[collectionConfig.collectionName] = {};
      for (let k = 0; k < collectionConfig.attributes.length; k++) {
        const { name, ...attribs } = collectionConfig.attributes[k];
        data[collectionConfig.collectionName][name] = attribs;
      }
      setIsInProgress(true);
      saveConfigForCollection(collectionConfig.collectionName, data)
        .then((resp) => {
          showMessage({
            variant: 'success',
            title: 'The collection configuration is saved.',
            text: 'The collection configuration is saved.',
          });
          window.scrollTo({ top: 0, behavior: 'smooth' });
        })
        .catch((err) => {
          showMessage({
            variant: 'warning',
            title: 'An error was encountered.',
            text: err.message || 'An error was encountered.',
          });
          window.scrollTo({ top: 0, behavior: 'smooth' });
          console.log(err);
        })
        .finally(() => setIsInProgress(false));
    }
  };

  useEffect(() => {
    loadTransformers().then((resp) => setTransformersList(resp));
  }, [selectedCollection]);

  useEffect(() => {
    if (params && params.collectionName) setSelectedCollection(params.collectionName);
  }, [params]);

  useEffect(() => {
    if (selectedCollection) {
      loadConfigForCollection(selectedCollection)
        .then((resp) => {
          if (Object.keys(resp).length === 0) {
            showMessage({
              variant: 'warning',
              title: 'No collection with the selected name exists.',
              text: 'No collection with the selected name exists.',
            });
          } else {
            const collectionName = Object.keys(resp)[0];
            const attributeNames = Object.keys(resp[collectionName]);
            const attributes = [];
            for (let s = 0; s < attributeNames.length; s++)
              attributes.push({
                name: attributeNames[s],
                ...resp[collectionName][attributeNames[s]],
              });
            const item = { collectionName, attributes };
            setCollectionConfig(item);
          }
        })
        .catch((err) => {
          showMessage({
            variant: 'warning',
            title: 'An error was encountered.',
            text: err.message || 'An error was encountered.',
          });
          window.scrollTo({ top: 0, behavior: 'smooth' });
          console.log(err);
        });
    }
  }, [selectedCollection]);

  if (collectionConfig === null) return <Loader />;
  else
    return (
      <Page.Main>
        <Page.Title>Configure Collection {selectedCollection}</Page.Title>
        <Flex alignItems="stretch" gap={4}>
          <SubNavigation activeUrl={`/plugins/${pluginId}/configure-collections`} />
          <Box padding={8} background="neutral100" width="100%">
            <Box paddingBottom={4}>
              <Link
                startIcon={<ArrowLeft />}
                href={`/admin/plugins/${pluginId}/configure-collections`}
              >
                Back
              </Link>
            </Box>
            {selectedCollection && (
              <Box paddingBottom={4}>
                <Typography variant="alpha">{selectedCollection}</Typography>
              </Box>
            )}
            {alertContent && (
              <Alert
                closeLabel="Close alert"
                title={alertContent.title}
                variant={alertContent.variant}
              >
                {alertContent.text}
              </Alert>
            )}
            {collectionConfig && (
              <>
                <Flex paddingTop={8} alignItems="stretch" gap={4} width="100%">
                  <Box padding={8} background="neutral0" width="100%">
                    <Box paddingBottom={2}>
                      <Typography variant="beta">Attributes</Typography>
                      {collectionConfig.attributes.map((a, idx) => {
                        return (
                          <Box paddingTop={4} paddingBottom={4}>
                            <ConfigureField
                              index={idx}
                              config={a}
                              setFieldConfig={updateCollectionsConfig}
                              transformersList={transformersList}
                            />
                          </Box>
                        );
                      })}
                    </Box>
                  </Box>
                </Flex>
                <Box paddingTop={4}>
                  <Button loading={isInProgress} variant="default" onClick={saveCollectionConfig}>
                    Save Configuration Changes
                  </Button>
                </Box>
              </>
            )}
          </Box>
        </Flex>
      </Page.Main>
    );
};

export default ConfigureCollection;
