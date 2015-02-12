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

package org.apache.ignite.internal;

import java.io.*;
import java.util.*;

/**
 * Ignite properties holder.
 */
public class GridProperties {
    /** Properties file path. */
    private static final String FILE_PATH = "ignite.properties";

    /** Properties. */
    private static final Properties PROPS;

    /**
     *
     */
    static {
        PROPS = new Properties();

        readProperties(FILE_PATH, true);
    }

    /**
     * @param path Path.
     * @param throwExc Flag indicating whether to throw an exception or not.
     */
    private static void readProperties(String path, boolean throwExc) {
        try (InputStream is = IgniteVersionUtils.class.getClassLoader().getResourceAsStream(path)) {
            if (is == null) {
                if (throwExc)
                    throw new RuntimeException("Failed to find properties file: " + path);
                else
                    return;
            }

            PROPS.load(is);
        }
        catch (IOException e) {
            throw new RuntimeException("Failed to read properties file: " + path, e);
        }
    }

    /**
     * Gets property value.
     *
     * @param key Property key.
     * @return Property value (possibly empty string, but never {@code null}).
     */
    public static String get(String key) {
        return PROPS.getProperty(key, "");
    }

    /**
     *
     */
    private GridProperties() {
        // No-op.
    }
}
