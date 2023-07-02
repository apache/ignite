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
package org.hawkore.ignite.lucene.schema.mapping;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.regex.Pattern;

import org.hawkore.ignite.lucene.IndexException;

/**
 * A {@link Mapper} to map inet addresses.
 *
 * @author Andres de la Pena {@literal <adelapena@stratio.com>}
 */
public class InetMapper extends KeywordMapper {

    private static final Pattern IPV4_PATTERN = Pattern.compile(
            "(([01]?\\d\\d?|2[0-4]\\d|25[0-5])\\.){3}([01]?\\d\\d?|2[0-4]\\d|25[0-5])");

    private static final Pattern IPV6_PATTERN = Pattern.compile("^(?:[0-9a-fA-F]{1,4}:){7}[0-9a-fA-F]{1,4}$");

    private static final Pattern IPV6_COMPRESSED_PATTERN = Pattern.compile(
            "^((?:[0-9A-Fa-f]{1,4}(?::[0-9A-Fa-f]{1,4})*)?)::((?:[0-9A-Fa-f]{1,4}(?::[0-9A-Fa-f]{1,4})*)?)$");

    /**
     * Builds a new {@link InetMapper}.
     *
     * @param field the name of the field
     * @param column the name of the column to be mapped
     * @param validated if the field must be validated
     */
    public InetMapper(String field, String column, Boolean validated) {
        super(field, column, validated, Arrays.asList(String.class, InetAddress.class));
    }

    /** {@inheritDoc} */
    @Override
    protected String doBase(String name, Object value) {
        if (value instanceof InetAddress) {
            return ((InetAddress) value).getHostAddress();
        } else if (value instanceof String) {
            return doBase(name, (String) value);
        } else {
            throw new IndexException("Field '{}' requires an inet address, but found '{}'", name, value);
        }
    }

    private String doBase(String name, String value) {
        if (IPV4_PATTERN.matcher(value).matches() ||
            IPV6_PATTERN.matcher(value).matches() ||
            IPV6_COMPRESSED_PATTERN.matcher(value).matches()) {
            try {
                return InetAddress.getByName(value).getHostAddress();
            } catch (UnknownHostException e) {
                throw new IndexException(e, "Unknown host exception for field '{}' with value '{}'", name, value);
            }
        }
        throw new IndexException("Field '{}' with value '{}' can not be parsed as an inet address", name, value);
    }
}
