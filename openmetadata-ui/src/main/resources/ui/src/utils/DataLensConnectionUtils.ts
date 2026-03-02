/*
 *  Copyright 2026 Collate.
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *  http://www.apache.org/licenses/LICENSE-2.0
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

import { cloneDeep, isString } from 'lodash';
import { ALL_ASTERISKS_REGEX } from '../constants/regex.constants';
import { DashboardServiceType } from '../generated/entity/data/dashboard';
import { ConfigData } from '../interface/service.interface';

export const DATALENS_AUTH_TYPE_IAM_TOKEN = 'IAM_TOKEN';
export const DATALENS_AUTH_TYPE_SERVICE_ACCOUNT_JSON = 'SERVICE_ACCOUNT_JSON';

export const toDataLensUiConnectionSchema = (
  connectionType: string,
  schema?: Record<string, unknown>
): Record<string, unknown> | undefined => {
  if (connectionType !== DashboardServiceType.DataLens || !schema) {
    return schema;
  }

  const normalizedSchema = cloneDeep(schema) as Record<string, unknown>;
  const properties = (normalizedSchema.properties ?? {}) as Record<
    string,
    unknown
  >;
  delete properties.iamToken;
  delete properties.serviceAccountJson;
  properties.authType = {
    title: 'Auth Type',
    description:
      'Choose whether credential is an IAM token or a Service Account JSON key.',
    type: 'string',
    enum: [
      DATALENS_AUTH_TYPE_IAM_TOKEN,
      DATALENS_AUTH_TYPE_SERVICE_ACCOUNT_JSON,
    ],
    default: DATALENS_AUTH_TYPE_IAM_TOKEN,
  };
  properties.authCredential = {
    title: 'Credential',
    description:
      'Paste IAM token or Service Account JSON based on selected auth type.',
    type: 'string',
    format: 'password',
  };

  normalizedSchema.properties = properties;
  delete normalizedSchema.anyOf;

  const required = Array.isArray(normalizedSchema.required)
    ? (normalizedSchema.required as string[])
    : [];
  normalizedSchema.required = [
    ...new Set(
      required
        .filter(
          (field) => field !== 'iamToken' && field !== 'serviceAccountJson'
        )
        .concat(['authType', 'authCredential'])
    ),
  ];

  return normalizedSchema;
};

const getCredentialValue = (value: unknown): string | undefined => {
  if (!isString(value)) {
    return undefined;
  }

  const trimmedValue = value.trim();
  if (!trimmedValue || ALL_ASTERISKS_REGEX.test(trimmedValue)) {
    return undefined;
  }

  return trimmedValue;
};

export const toDataLensUiAuthConfig = (
  connectionType: string,
  config?: ConfigData
): ConfigData | undefined => {
  if (connectionType !== DashboardServiceType.DataLens || !config) {
    return config;
  }

  const normalizedConfig = cloneDeep(config) as Record<string, unknown>;
  const iamToken = isString(normalizedConfig.iamToken)
    ? normalizedConfig.iamToken.trim()
    : undefined;
  const serviceAccountJson = isString(normalizedConfig.serviceAccountJson)
    ? normalizedConfig.serviceAccountJson.trim()
    : undefined;

  const useServiceAccountJson = Boolean(serviceAccountJson) && !iamToken;
  normalizedConfig.authType = useServiceAccountJson
    ? DATALENS_AUTH_TYPE_SERVICE_ACCOUNT_JSON
    : DATALENS_AUTH_TYPE_IAM_TOKEN;
  normalizedConfig.authCredential = useServiceAccountJson
    ? serviceAccountJson
    : iamToken;

  delete normalizedConfig.iamToken;
  delete normalizedConfig.serviceAccountJson;

  return normalizedConfig as ConfigData;
};

/**
 * Normalize DataLens auth values:
 * - Masked values like "********" are treated as absent.
 * - For test-connection we drop absent auth fields.
 * - For save flow we can explicitly nullify the alternative auth field.
 */
export const normalizeDataLensAuthConfig = (
  connectionType: string,
  config?: ConfigData,
  clearAlternativeCredential = false
): ConfigData | undefined => {
  if (connectionType !== DashboardServiceType.DataLens || !config) {
    return config;
  }

  const normalizedConfig = cloneDeep(config) as Record<string, unknown>;
  const rawAuthType = getCredentialValue(normalizedConfig.authType);
  const rawAuthCredential = isString(normalizedConfig.authCredential)
    ? normalizedConfig.authCredential.trim()
    : undefined;
  const authCredential = getCredentialValue(rawAuthCredential);
  let iamToken = getCredentialValue(normalizedConfig.iamToken);
  let serviceAccountJson = getCredentialValue(
    normalizedConfig.serviceAccountJson
  );

  if (authCredential) {
    if (rawAuthType === DATALENS_AUTH_TYPE_SERVICE_ACCOUNT_JSON) {
      serviceAccountJson = authCredential;
      iamToken = undefined;
    } else {
      iamToken = authCredential;
      serviceAccountJson = undefined;
    }
  } else if (
    clearAlternativeCredential &&
    rawAuthCredential &&
    ALL_ASTERISKS_REGEX.test(rawAuthCredential)
  ) {
    if (rawAuthType === DATALENS_AUTH_TYPE_SERVICE_ACCOUNT_JSON) {
      serviceAccountJson = rawAuthCredential;
      iamToken = undefined;
    } else {
      iamToken = rawAuthCredential;
      serviceAccountJson = undefined;
    }
  }

  if (iamToken) {
    normalizedConfig.iamToken = iamToken;
    if (!serviceAccountJson) {
      if (clearAlternativeCredential) {
        normalizedConfig.serviceAccountJson = null;
      } else {
        delete normalizedConfig.serviceAccountJson;
      }
    }
  }

  if (serviceAccountJson) {
    normalizedConfig.serviceAccountJson = serviceAccountJson;
    if (!iamToken) {
      if (clearAlternativeCredential) {
        normalizedConfig.iamToken = null;
      } else {
        delete normalizedConfig.iamToken;
      }
    }
  }

  if (!clearAlternativeCredential && !iamToken && !serviceAccountJson) {
    delete normalizedConfig.iamToken;
    delete normalizedConfig.serviceAccountJson;
  }

  delete normalizedConfig.authType;
  delete normalizedConfig.authCredential;

  return normalizedConfig as ConfigData;
};
