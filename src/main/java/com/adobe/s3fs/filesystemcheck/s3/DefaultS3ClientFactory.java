/*
Copyright 2026 Adobe. All rights reserved.
This file is licensed to you under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License. You may obtain a copy
of the License at https://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software distributed under
the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR REPRESENTATIONS
OF ANY KIND, either express or implied. See the License for the specific language
governing permissions and limitations under the License.
*/
package com.adobe.s3fs.filesystemcheck.s3;

import software.amazon.awssdk.core.retry.RetryPolicy;
import software.amazon.awssdk.http.apache.ApacheHttpClient;
import software.amazon.awssdk.services.s3.S3Client;

public class DefaultS3ClientFactory implements S3ClientFactory {

    /**
     * Single instance of the factory.
     */
    public static final S3ClientFactory INSTANCE = new DefaultS3ClientFactory();

    /**
     * Singleton class, so no public constructor.
     */
    private DefaultS3ClientFactory() {}

    @Override
    public S3Client newS3Client(RetryPolicy retryPolicy, int maxConnections) {
        return S3Client.builder()
                .httpClientBuilder(ApacheHttpClient.builder().maxConnections(maxConnections))
                .overrideConfiguration(config -> config.retryPolicy(retryPolicy))
                .build();
    }
}
