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

import { DashboardServiceType } from '../generated/entity/data/dashboard';
import { ConfigData } from '../interface/service.interface';
import {
  DATALENS_AUTH_TYPE_IAM_TOKEN,
  DATALENS_AUTH_TYPE_SERVICE_ACCOUNT_JSON,
  normalizeDataLensAuthConfig,
  toDataLensUiAuthConfig,
  toDataLensUiConnectionSchema,
} from './DataLensConnectionUtils';

describe('DataLensConnectionUtils', () => {
  it('should clear serviceAccountJson when iamToken is provided for save flow', () => {
    const config = {
      iamToken: 'iam-token',
      serviceAccountJson: '********',
      organizationId: 'org-id',
    };

    const result = normalizeDataLensAuthConfig(
      DashboardServiceType.DataLens,
      config as unknown as ConfigData,
      true
    ) as unknown as Record<string, unknown>;

    expect(result.iamToken).toBe('iam-token');
    expect(result.serviceAccountJson).toBeNull();
  });

  it('should clear iamToken when serviceAccountJson is provided for save flow', () => {
    const config = {
      iamToken: '********',
      serviceAccountJson: '{"id":"kid"}',
      organizationId: 'org-id',
    };

    const result = normalizeDataLensAuthConfig(
      DashboardServiceType.DataLens,
      config as unknown as ConfigData,
      true
    ) as unknown as Record<string, unknown>;

    expect(result.serviceAccountJson).toBe('{"id":"kid"}');
    expect(result.iamToken).toBeNull();
  });

  it('should drop masked auth values for test connection flow', () => {
    const config = {
      iamToken: '********',
      serviceAccountJson: '********',
      organizationId: 'org-id',
    };

    const result = normalizeDataLensAuthConfig(
      DashboardServiceType.DataLens,
      config as unknown as ConfigData
    ) as unknown as Record<string, unknown>;

    expect(result.iamToken).toBeUndefined();
    expect(result.serviceAccountJson).toBeUndefined();
  });

  it('should keep config unchanged for non-datalens connections', () => {
    const config = {
      iamToken: 'iam-token',
      serviceAccountJson: '********',
      organizationId: 'org-id',
    };

    const result = normalizeDataLensAuthConfig(
      'Mysql',
      config as unknown as ConfigData
    ) as unknown as Record<string, unknown>;

    expect(result).toEqual(config);
  });

  it('should map UI auth fields to iamToken', () => {
    const config = {
      authType: DATALENS_AUTH_TYPE_IAM_TOKEN,
      authCredential: 'iam-token',
      organizationId: 'org-id',
    };

    const result = normalizeDataLensAuthConfig(
      DashboardServiceType.DataLens,
      config as unknown as ConfigData,
      true
    ) as unknown as Record<string, unknown>;

    expect(result.iamToken).toBe('iam-token');
    expect(result.serviceAccountJson).toBeNull();
    expect(result.authType).toBeUndefined();
    expect(result.authCredential).toBeUndefined();
  });

  it('should map UI auth fields to serviceAccountJson', () => {
    const config = {
      authType: DATALENS_AUTH_TYPE_SERVICE_ACCOUNT_JSON,
      authCredential: '{"id":"kid"}',
      organizationId: 'org-id',
    };

    const result = normalizeDataLensAuthConfig(
      DashboardServiceType.DataLens,
      config as unknown as ConfigData,
      true
    ) as unknown as Record<string, unknown>;

    expect(result.serviceAccountJson).toBe('{"id":"kid"}');
    expect(result.iamToken).toBeNull();
    expect(result.authType).toBeUndefined();
    expect(result.authCredential).toBeUndefined();
  });

  it('should map stored iam token fields to UI auth fields', () => {
    const config = {
      iamToken: 'iam-token',
      organizationId: 'org-id',
    };

    const result = toDataLensUiAuthConfig(
      DashboardServiceType.DataLens,
      config as unknown as ConfigData
    ) as unknown as Record<string, unknown>;

    expect(result.authType).toBe(DATALENS_AUTH_TYPE_IAM_TOKEN);
    expect(result.authCredential).toBe('iam-token');
    expect(result.iamToken).toBeUndefined();
  });

  it('should map DataLens schema auth fields to unified UI fields', () => {
    const schema = {
      properties: {
        organizationId: { type: 'string' },
        iamToken: { type: 'string' },
        serviceAccountJson: { type: 'string' },
      },
      required: ['organizationId', 'iamToken'],
      anyOf: [{ required: ['iamToken'] }, { required: ['serviceAccountJson'] }],
    };

    const result = toDataLensUiConnectionSchema(
      DashboardServiceType.DataLens,
      schema as unknown as Record<string, unknown>
    ) as Record<string, unknown>;
    const properties = result.properties as Record<string, unknown>;
    const required = result.required as string[];

    expect(properties.iamToken).toBeUndefined();
    expect(properties.serviceAccountJson).toBeUndefined();
    expect(properties.authType).toBeDefined();
    expect(properties.authCredential).toBeDefined();
    expect(result.anyOf).toBeUndefined();
    expect(required).toContain('organizationId');
    expect(required).toContain('authType');
    expect(required).toContain('authCredential');
  });
});
