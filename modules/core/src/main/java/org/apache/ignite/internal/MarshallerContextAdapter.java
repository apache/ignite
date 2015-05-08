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

import org.apache.ignite.*;
import org.apache.ignite.internal.util.typedef.internal.*;
import org.apache.ignite.marshaller.*;
import org.jsr166.*;

import java.io.*;
import java.net.*;
import java.util.*;
import java.util.concurrent.*;

/**
 * Marshaller context adapter.
 */
public abstract class MarshallerContextAdapter implements MarshallerContext {
    /** */
    private static final String CLS_NAMES_FILE = "META-INF/classnames.properties";

    /** */
    private static final String JDK_CLS_NAMES_FILE = "META-INF/classnames-jdk.properties";

    /** */
    private final ConcurrentMap<Integer, String> map = new ConcurrentHashMap8<>();

    /**
     * Initializes context.
     */
    public MarshallerContextAdapter() {
        try {
            ClassLoader ldr = U.gridClassLoader();

            Enumeration<URL> urls = ldr.getResources(CLS_NAMES_FILE);

            while (urls.hasMoreElements())
                processResource(urls.nextElement());

            processResource(ldr.getResource(JDK_CLS_NAMES_FILE));
        }
        catch (IOException e) {
            throw new IllegalStateException("Failed to initialize marshaller context.", e);
        }
    }

    /**
     * @param url Resource URL.
     * @throws IOException In case of error.
     */
    private void processResource(URL url) throws IOException {
        try (InputStream in = url.openStream()) {
            BufferedReader rdr = new BufferedReader(new InputStreamReader(in));

            String line;

            while ((line = rdr.readLine()) != null) {
                if (line.isEmpty() || line.startsWith("#"))
                    continue;

                String clsName = line.trim();

                map.put(clsName.hashCode(), clsName);
            }
        }
    }

    /** {@inheritDoc} */
    @Override public boolean registerClass(int id, Class cls) throws IgniteCheckedException {
        boolean registered = true;

        if (!map.containsKey(id)) {
            registered = registerClassName(id, cls.getName());

            if (registered)
                map.putIfAbsent(id, cls.getName());
        }

        return registered;
    }

    /** {@inheritDoc} */
    @Override public Class getClass(int id, ClassLoader ldr) throws ClassNotFoundException {
        String clsName = map.get(id);

        if (clsName == null) {
            clsName = className(id);

            assert clsName != null : id;

            String old = map.putIfAbsent(id, clsName);

            if (old != null)
                clsName = old;
        }

        return U.forName(clsName, ldr);
    }

    /**
     * Registers class name.
     *
     * @param id Type ID.
     * @param clsName Class name.
     * @return Whether class name was registered.
     */
    protected abstract boolean registerClassName(int id, String clsName) throws IgniteCheckedException;

    /**
     * Gets class name by type ID.
     *
     * @param id Type ID.
     * @return Class name.
     */
    protected abstract String className(int id);
}
