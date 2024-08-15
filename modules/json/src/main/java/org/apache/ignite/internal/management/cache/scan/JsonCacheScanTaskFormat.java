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

package org.apache.ignite.internal.management.cache.scan;

import java.util.Collections;
import java.util.List;
import javax.cache.Cache;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.jackson.IgniteObjectMapper;

/**
 * This format prints cache objects in json format.
 */
public class JsonCacheScanTaskFormat implements CacheScanTaskFormat {
    /** */
    public static final String NAME = "json";

    /** */
    private final ObjectMapper mapper = new IgniteObjectMapper();

    /** {@inheritDoc} */
    @Override public String name() {
        return NAME;
    }

    /** {@inheritDoc} */
    @Override public List<String> titles(Cache.Entry<Object, Object> first) {
        return Collections.singletonList("data");
    }

    /** {@inheritDoc} */
    @Override public List<?> row(Cache.Entry<Object, Object> e) {
        try {
            return Collections.singletonList(mapper.writeValueAsString(e));
        }
        catch (JsonProcessingException ex) {
            throw new IgniteException(ex);
        }
    }
}
