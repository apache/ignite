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

package org.apache.ignite.plugin.security;

import org.apache.ignite.IgniteCheckedException;

/**
 * Basic implementation for {@link SecurityCredentialsProvider}. Use it
 * when custom logic for storing security credentials is not required and it
 * is OK to specify credentials directly in configuration.
 */
public class SecurityCredentialsBasicProvider implements SecurityCredentialsProvider {
    /** */
    private SecurityCredentials cred;

    /**
     * Constructs security credentials provider based on security credentials passed in.
     *
     * @param cred Security credentials.
     */
    public SecurityCredentialsBasicProvider(SecurityCredentials cred) {
        this.cred = cred;
    }

    /** {@inheritDoc} */
    @Override public SecurityCredentials credentials() throws IgniteCheckedException {
        return cred;
    }
}