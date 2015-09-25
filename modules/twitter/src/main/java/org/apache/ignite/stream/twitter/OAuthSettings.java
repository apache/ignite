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

package org.apache.ignite.stream.twitter;

import org.jetbrains.annotations.NotNull;

/**
 * OAuth keys holder.
 */
public class OAuthSettings {

    @NotNull private String consumerKey;
    @NotNull private String consumerSecret;
    @NotNull private String accessToken;
    @NotNull private String accessTokenSecret;

    public OAuthSettings(String consumerKey, String consumerSecret, String accessToken, String accessTokenSecret) {
        this.consumerKey = consumerKey;
        this.consumerSecret = consumerSecret;
        this.accessToken = accessToken;
        this.accessTokenSecret = accessTokenSecret;
    }

    @NotNull
    public String getConsumerKey() {
        return consumerKey;
    }

    @NotNull
    public String getConsumerSecret() {
        return consumerSecret;
    }

    @NotNull
    public String getAccessToken() {
        return accessToken;
    }

    @NotNull
    public String getAccessTokenSecret() {
        return accessTokenSecret;
    }
}
