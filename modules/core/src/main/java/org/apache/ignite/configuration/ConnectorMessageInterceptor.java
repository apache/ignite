/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.configuration;

/**
 * Interface for user-defined object interceptors.
 * <p>
 * Interceptors allow user to transform objects send and received via REST protocols.
 * For example they could be used for customized multi-language marshalling by
 * converting binary object representation received from client to java object.
 */
public interface ConnectorMessageInterceptor {
    /**
     * Intercepts received objects.
     *
     * @param obj Original incoming object.
     * @return Object which should replace original in later processing.
     */
    public Object onReceive(Object obj);

    /**
     * Intercepts received objects.
     *
     * @param obj Original incoming object.
     * @return Object which should be send to remote client instead of original.
     */
    public Object onSend(Object obj);
}