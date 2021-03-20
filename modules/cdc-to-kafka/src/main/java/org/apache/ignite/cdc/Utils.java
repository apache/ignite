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

package org.apache.ignite.cdc;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashSet;
import java.util.Objects;
import java.util.Properties;
import java.util.Set;

/**
 * Utility class.
 */
public class Utils {
    /**
     * Reads property from properties.
     *
     * @param propName Property name.
     * @param props Properties.
     * @return Property value, {@code null} if property don't exists.
     */
    public static String property(String propName, Properties props) {
        return property(propName, props, null);
    }

    /**
     * Reads property from properties.
     *
     * @param propName Property name.
     * @param props Properties.
     * @param def Default value.
     * @return Property value.
     */
    public static String property(String propName, Properties props, String def) {
        String val = System.getProperty(propName);

        if (val != null)
            return val;

        val = props.getProperty(propName);

        if (val != null)
            return val;

        return def;
    }

    /**
     * Reads properties from file.
     *
     * @param path Path to properties.
     * @param errMsg Error message.
     * @return Properties from file.
     */
    public static Properties properties(String path, String errMsg) throws IOException {
        File propsFile = new File(Objects.requireNonNull(path, errMsg));

        if (!propsFile.exists())
            throw new IllegalArgumentException(errMsg);

        try (InputStream in = new FileInputStream(propsFile)) {
            Properties props = new Properties();

            props.load(in);

            return props;
        }
    }

    /**
     * Parses simple partitions patterns.
     * Each single pattern divided by comma.
     * The following signle pattern supported:
     * 1. Single partition number - "1" or "42".
     * 2. Partition interval - "1-42", "42-45" which means all partitions from first to second number, inclusive.
     *
     * @param partsStr Pattern string
     * @return Set of the partitions.
     */
    public static Set<Integer> partitions(String partsStr) {
        String[] singlePartsPatterns = partsStr.split(",");

        Set<Integer> parts = new HashSet<>();

        for (String pattern : singlePartsPatterns) {
            int delimIdx = pattern.indexOf('-');
            if (delimIdx != -1) {
                int first = Integer.parseInt(pattern.substring(0, delimIdx));
                int second = Integer.parseInt(pattern.substring(delimIdx + 1));

                if (first > second) {
                    throw new IllegalArgumentException("Pattern " + pattern + " is wrong. " +
                        "Second number must be greater or equals then first.");
                }

                for (int i = first; i <= second; i++)
                    parts.add(i);
            }
            else
                parts.add(Integer.parseInt(pattern));
        }

        if (parts.isEmpty())
            throw new IllegalArgumentException("Partitions string is empty!");

        return parts;
    }
}
