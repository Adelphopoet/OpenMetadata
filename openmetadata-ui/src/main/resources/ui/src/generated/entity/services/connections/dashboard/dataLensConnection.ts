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
/**
 * DataLens Connection Config
 */
export interface DataLensConnection {
    /**
     * DataLens API base URL.
     */
    apiURL?: string;
    /**
     * DataLens API version header.
     */
    apiVersion?: string;
    /**
     * Regex exclude or include charts that matches the pattern.
     */
    chartFilterPattern?: FilterPattern;
    /**
     * Substring match for collection name. When set, only dashboards from matching collections are ingested.
     */
    collectionNamePattern?: string;
    /**
     * Regex to exclude or include dashboards that matches the pattern.
     */
    dashboardFilterPattern?: FilterPattern;
    /**
     * Regex exclude or include data models that matches the pattern.
     */
    dataModelFilterPattern?: FilterPattern;
    /**
     * Base URL for the DataLens UI (optional, used for source links).
     */
    hostPort?: string;
    /**
     * IAM token for DataLens API access.
     */
    iamToken?: string;
    /**
     * Service account authorized key JSON. If provided, IAM token will be generated automatically at ingestion start.
     */
    serviceAccountJson?: string;
    /**
     * Organization ID for DataLens API access.
     */
    organizationId: string;
    /**
     * Page size for pagination in API requests. Default is 100.
     */
    pageSize?: number;
    /**
     * When enabled, ingest only dashboards that belong to a collection (directly or via workbook).
     */
    onlyDashboardsInCollections?: boolean;
    /**
     * Regex to exclude or include projects that matches the pattern.
     */
    projectFilterPattern?: FilterPattern;
    /**
     * Maximum retries after rate limit responses.
     */
    rateLimitMaxRetries?: number;
    /**
     * Delay before retrying after a rate limit response.
     */
    rateLimitRetrySeconds?: number;
    /**
     * Minimum delay between API requests to avoid rate limiting.
     */
    requestDelaySeconds?:        number;
    supportsMetadataExtraction?: boolean;
    /**
     * Service Type
     */
    type?: DataLensType;
    /**
     * Boolean marking if we need to verify the SSL certs for DataLens. Default to True.
     */
    verifySSL?: boolean;
}

/**
 * Regex exclude or include charts that matches the pattern.
 *
 * Regex to only fetch entities that matches the pattern.
 *
 * Regex to exclude or include dashboards that matches the pattern.
 *
 * Regex exclude or include data models that matches the pattern.
 *
 * Regex to exclude or include projects that matches the pattern.
 */
export interface FilterPattern {
    /**
     * List of strings/regex patterns to match and exclude only database entities that match.
     */
    excludes?: string[];
    /**
     * List of strings/regex patterns to match and include only database entities that match.
     */
    includes?: string[];
}

/**
 * Service Type
 *
 * DataLens service type
 */
export enum DataLensType {
    DataLens = "DataLens",
}
