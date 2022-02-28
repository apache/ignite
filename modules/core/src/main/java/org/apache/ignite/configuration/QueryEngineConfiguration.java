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

package org.apache.ignite.configuration;

/**
 * Interface for the configuration of the query engine.
 */
public interface QueryEngineConfiguration {
    /**
     * Sets whether this query engine should be used by default.
     * <p>
     * There can be only one query engine configuration with the default flag.
     * <p>
     * If there is no configuration with the default flag, the query engine provided by the ignite-indexing module
     * will be used by default (if configured). If there is no configuration for the ignite-indexing module engine
     * exists, the first engine from the query engines configuration will be used.
     *
     * @param isDflt {@code True} if this query engine should be used by default.
     * @return {@code this} for chaining.
     */
    public QueryEngineConfiguration setDefault(boolean isDflt);

    /**
     * Is this query engine should be used by default.
     *
     * @return {@code True} if this query engine is default.
     */
    public boolean isDefault();
}
