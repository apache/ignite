/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.util;

import org.apache.ignite.internal.util.typedef.X;

/**
 * S3 tests configuration.
 */
public final class IgniteS3TestConfiguration {
    /**
     * @return Access key.
     */
    public static String getAccessKey() {
        return getRequiredEnvVar("test.amazon.access.key");
    }

    /**
     * @return Access key.
     */
    public static String getSecretKey() {
        return getRequiredEnvVar("test.amazon.secret.key");
    }

    /**
     * @param dfltBucketName Default bucket name.
     * @return Bucket name.
     */
    public static String getBucketName(final String dfltBucketName) {
        String val = X.getSystemOrEnv("test.s3.bucket.name");

        return val == null ? dfltBucketName : val;
    }

    /**
     * @param name Name of environment.
     * @return Environment variable value.
     */
    private static String getRequiredEnvVar(String name) {
        String key = X.getSystemOrEnv(name);

        assert key != null : String.format("Environment variable '%s' is not set", name);

        return key;
    }
}
