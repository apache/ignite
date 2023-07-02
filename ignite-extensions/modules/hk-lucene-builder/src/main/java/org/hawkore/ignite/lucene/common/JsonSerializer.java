/*
 * Copyright (C) 2014 Stratio (http://stratio.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.hawkore.ignite.lucene.common;

import java.io.IOException;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * A JSON mapper based on Codehaus {@link ObjectMapper} annotations.
 *
 * @author Andres de la Pena {@literal <adelapena@stratio.com>}
 */
public final class JsonSerializer {

    public static final JsonSerializer INSTANCE = new JsonSerializer();

    /** The embedded JSON serializer. */
    public final ObjectMapper mapper = new ObjectMapper();

    /** Private constructor to hide the implicit public one. */
    private JsonSerializer() {
        mapper.configure(JsonGenerator.Feature.QUOTE_FIELD_NAMES, true);
        mapper.configure(JsonParser.Feature.ALLOW_UNQUOTED_FIELD_NAMES, true);
        mapper.configure(JsonParser.Feature.ALLOW_SINGLE_QUOTES, true);
        mapper.configure(JsonParser.Feature.ALLOW_UNQUOTED_CONTROL_CHARS, true);
        mapper.configure(MapperFeature.AUTO_DETECT_IS_GETTERS, false);
        mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, true);
        mapper.configure(DeserializationFeature.ACCEPT_SINGLE_VALUE_AS_ARRAY, true);
        mapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);
    }

    /**
     * Returns the JSON {@code String} representation of the specified object.
     *
     * @param value the object to be serialized.
     * @return the JSON {@code String} representation of {@code value}
     * @throws IOException if there are serialization problems
     */
    public static String toString(Object value) throws IOException {
        return INSTANCE.mapper.writeValueAsString(value);
    }

    /**
     * Returns the object of the specified class represented by the specified JSON {@code String}.
     *
     * @param value the JSON {@code String} to be parsed
     * @param valueType the class of the object to be parsed
     * @param <T> the type of the object to be parsed
     * @return an object of the specified class represented by {@code value}
     * @throws IOException if there are parsing problems
     */
    public static <T> T fromString(String value, Class<T> valueType) throws IOException {
        return INSTANCE.mapper.readValue(value, valueType);
    }
}
