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

package org.apache.ignite.internal.processors.rest.protocols.http.jetty;

import java.util.UUID;
import net.sf.json.JsonConfig;
import net.sf.json.processors.JsonValueProcessor;

/**
 * Jetty protocol json configuration.
 */
public class GridJettyJsonConfig extends JsonConfig {
    /**
     * Constructs default jetty json config.
     */
    public GridJettyJsonConfig() {
        registerJsonValueProcessor(UUID.class, new ToStringJsonProcessor());
    }

    /**
     * Helper class for simple to-string conversion for the beans.
     */
    private static class ToStringJsonProcessor implements JsonValueProcessor {
        /** {@inheritDoc} */
        @Override public Object processArrayValue(Object val, JsonConfig jsonCfg) {
            throw new UnsupportedOperationException("Serialize array to string is not supported: " + val);
        }

        /** {@inheritDoc} */
        @Override public Object processObjectValue(String key, Object val, JsonConfig jsonCfg) {
            return val == null ? null : val.toString();
        }
    }
}