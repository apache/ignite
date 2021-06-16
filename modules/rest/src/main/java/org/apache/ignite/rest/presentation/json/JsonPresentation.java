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

package org.apache.ignite.rest.presentation.json;

import java.util.Collections;
import java.util.concurrent.ExecutionException;
import com.google.gson.JsonElement;
import com.google.gson.JsonParser;
import org.apache.ignite.internal.configuration.ConfigurationRegistry;
import org.apache.ignite.internal.configuration.json.JsonConverter;
import org.apache.ignite.internal.configuration.util.ConfigurationUtil;
import org.apache.ignite.rest.presentation.ConfigurationPresentation;

/** */
public class JsonPresentation implements ConfigurationPresentation<String> {
    /** */
    private final ConfigurationRegistry sysCfg;

    /**
     * @param sysCfg Configuration registry.
     */
    public JsonPresentation(ConfigurationRegistry sysCfg) {
        this.sysCfg = sysCfg;
    }

    /** {@inheritDoc} */
    @Override public String represent() {
        return sysCfg.represent(Collections.emptyList(), JsonConverter.jsonVisitor()).toString();
    }

    /** {@inheritDoc} */
    @Override public String representByPath(String path) {
        if (path == null || path.isEmpty())
            return represent();

        return sysCfg.represent(ConfigurationUtil.split(path), JsonConverter.jsonVisitor()).toString();
    }

    /** {@inheritDoc} */
    @Override public void update(String configUpdate) {
        JsonElement jsonUpdate = JsonParser.parseString(configUpdate);

        try {
            sysCfg.change(JsonConverter.jsonSource(jsonUpdate), null).get();
        }
        catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        catch (ExecutionException e) {
            throw new RuntimeException(e.getCause());
        }
    }
}
