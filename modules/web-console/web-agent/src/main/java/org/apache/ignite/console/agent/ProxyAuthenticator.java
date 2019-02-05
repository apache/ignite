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

package org.apache.ignite.console.agent;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.PasswordAuthentication;
import java.net.Proxy;
import java.util.List;
import okhttp3.Authenticator;
import okhttp3.Challenge;
import okhttp3.Credentials;
import okhttp3.HttpUrl;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;
import okhttp3.Route;

import static java.net.Authenticator.RequestorType.PROXY;

/**
 * Request interactive proxy credentials.
 *
 * Configure OkHttp to use {@link OkHttpClient.Builder#proxyAuthenticator(Authenticator)}.
 */
public class ProxyAuthenticator implements Authenticator {
    /** Latest credential hash code. */
    private int latestCredHashCode = 0;

    /** {@inheritDoc} */
    @Override public Request authenticate(Route route, Response res) throws IOException {
        List<Challenge> challenges = res.challenges();

        for (Challenge challenge : challenges) {
            if (!"Basic".equalsIgnoreCase(challenge.scheme()))
                continue;

            Request req = res.request();
            HttpUrl url = req.url();
            Proxy proxy = route.proxy();

            InetSocketAddress proxyAddr = (InetSocketAddress)proxy.address();

            PasswordAuthentication auth = java.net.Authenticator.requestPasswordAuthentication(
                proxyAddr.getHostName(), proxyAddr.getAddress(), proxyAddr.getPort(),
                url.scheme(), challenge.realm(), challenge.scheme(), url.url(), PROXY);

            if (auth != null) {
                String cred = Credentials.basic(auth.getUserName(), new String(auth.getPassword()), challenge.charset());

                if (latestCredHashCode == cred.hashCode()) {
                    latestCredHashCode = 0;

                    throw new ProxyAuthException("Failed to authenticate with proxy");
                }

                latestCredHashCode = cred.hashCode();

                return req.newBuilder()
                    .header("Proxy-Authorization", cred)
                    .build();
            }
        }

        return null; // No challenges were satisfied!
    }
}
