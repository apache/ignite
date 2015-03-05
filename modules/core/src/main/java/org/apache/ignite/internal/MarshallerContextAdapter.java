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

import org.apache.ignite.marshaller.*;
import org.jdk8.backport.*;

import java.io.*;
import java.util.concurrent.*;

/**
 * Marshaller context adapter.
 */
public abstract class MarshallerContextAdapter implements MarshallerContext {
    /** */
    private static final String CLS_NAMES_FILE = "org/apache/ignite/internal/classnames.properties";

    /** */
    protected final ConcurrentMap<Integer, String> clsNameById = new ConcurrentHashMap8<>();

    /**
     * Initializes context.
     */
    public MarshallerContextAdapter() {
        try {
            ClassLoader ldr = getClass().getClassLoader();

            BufferedReader rdr = new BufferedReader(new InputStreamReader(ldr.getResourceAsStream(CLS_NAMES_FILE)));

            String line;

            while ((line = rdr.readLine()) != null) {
                if (line.isEmpty() || line.startsWith("#"))
                    continue;

                String clsName = line.trim();

                clsNameById.put(clsName.hashCode(), clsName);
            }
        }
        catch (IOException e) {
            throw new IllegalStateException("Failed to initialize marshaller context.", e);
        }
    }
}
