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

package org.apache.ignite.marshaller;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URL;
import java.util.Enumeration;
import java.util.function.Consumer;
import org.apache.ignite.IgniteCommonsSystemProperties;
import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.ClassSet;
import org.apache.ignite.internal.util.CommonUtils;

import static org.apache.ignite.IgniteCommonsSystemProperties.IGNITE_MARSHALLER_BLACKLIST;

/**
 * Utility marshaller methods.
 */
public class MarshallerUtils {
    /** Jdk class names file. */
    public static final String JDK_CLS_NAMES_FILE = "META-INF/classnames-jdk.properties";

    /** Class names file. */
    public static final String CLS_NAMES_FILE = "META-INF/classnames.properties";

    /** Default black list class names file. */
    public static final String DEFAULT_BLACKLIST_CLS_NAMES_FILE = "META-INF/classnames-default-blacklist.properties";

    /** Default white list class names file. */
    public static final String DEFAULT_WHITELIST_CLS_NAMES_FILE = "META-INF/classnames-default-whitelist.properties";

    /** */
    private static final IgniteMarshallerClassFilter IGNITE_MARSHALLER_CLASS_FILTER
        = new IgniteMarshallerClassFilter(classWhiteList(), classBlackList());

    /**
     * Private constructor.
     */
    private MarshallerUtils() {
        // No-op.
    }

    /**
     * Returns class name filter for marshaller.
     *
     * @return Class name filter for marshaller.
     */
    public static IgniteMarshallerClassFilter classNameFilter() {
        return IGNITE_MARSHALLER_CLASS_FILTER;
    }

    /**
     * @return White list of classes.
     */
    private static ClassSet classWhiteList() {
        ClassSet clsSet = null;

        String fileName = IgniteCommonsSystemProperties.getString(IgniteCommonsSystemProperties.IGNITE_MARSHALLER_WHITELIST);

        if (fileName != null && !fileName.isBlank()) {
            clsSet = new ClassSet();

            addClassNames(JDK_CLS_NAMES_FILE, clsSet, CommonUtils.gridClassLoader());
            addClassNames(CLS_NAMES_FILE, clsSet, CommonUtils.gridClassLoader());
            addClassNames(DEFAULT_WHITELIST_CLS_NAMES_FILE, clsSet, CommonUtils.gridClassLoader());
            addClassNames(fileName, clsSet, CommonUtils.gridClassLoader());
        }

        return clsSet;
    }

    /**
     * @return Black list of classes.
     */
    private static ClassSet classBlackList() {
        ClassSet clsSet = new ClassSet();

        addClassNames(DEFAULT_BLACKLIST_CLS_NAMES_FILE, clsSet, CommonUtils.gridClassLoader());

        String blackListFileName = IgniteCommonsSystemProperties.getString(IGNITE_MARSHALLER_BLACKLIST);

        if (blackListFileName != null && !blackListFileName.isBlank())
            addClassNames(blackListFileName, clsSet, CommonUtils.gridClassLoader());

        return clsSet;
    }

    /**
     * Reads class names from resource referred by given system property name and returns set of classes.
     *
     * @param fileName File name containing list of classes.
     * @param clsSet Class set for update.
     * @param clsLdr Class loader.
     */
    private static void addClassNames(
        String fileName,
        ClassSet clsSet,
        ClassLoader clsLdr
    ) {
        InputStream is = clsLdr.getResourceAsStream(fileName);

        if (is == null) {
            try {
                is = new FileInputStream(new File(fileName));
            }
            catch (FileNotFoundException e) {
                throw new IgniteException("File " + fileName + " not found.");
            }
        }

        try (BufferedReader reader = new BufferedReader(new InputStreamReader(is))) {
            String line;

            for (int i = 1; (line = reader.readLine()) != null; i++) {
                String s = line.trim();

                if (!s.isEmpty() && s.charAt(0) != '#' && s.charAt(0) != '[') {
                    try {
                        clsSet.add(s);
                    }
                    catch (IllegalArgumentException e) {
                        throw new IgniteException("Exception occurred while reading list of classes" +
                            "[path=" + fileName + ", row=" + i + ", line=" + s + ']', e);
                    }
                }
            }
        }
        catch (IOException e) {
            throw new IgniteException("Exception occurred while reading and creating list of classes " +
                "[path=" + fileName + ']', e);
        }
    }

    /**
     * Find all system class names (for JDK or Ignite classes) and process them with a given consumer.
     *
     * @param proc Class processor (class name consumer).
     */
    public static void processSystemClasses(Consumer<String> proc) throws IOException {
        Enumeration<URL> urls = CommonUtils.gridClassLoader().getResources(CLS_NAMES_FILE);

        boolean foundClsNames = false;

        while (urls.hasMoreElements()) {
            processResource(urls.nextElement(), proc);

            foundClsNames = true;
        }

        if (!foundClsNames)
            throw new IgniteException("Failed to load class names properties file packaged with ignite binaries " +
                "[file=" + CLS_NAMES_FILE + ", ldr=" + CommonUtils.gridClassLoader() + ']');

        URL jdkClsNames = CommonUtils.gridClassLoader().getResource(JDK_CLS_NAMES_FILE);

        if (jdkClsNames == null)
            throw new IgniteException("Failed to load class names properties file packaged with ignite binaries " +
                "[file=" + JDK_CLS_NAMES_FILE + ", ldr=" + CommonUtils.gridClassLoader() + ']');

        processResource(jdkClsNames, proc);
    }

    /**
     * Process resource containing class names.
     *
     * @param url Resource URL.
     * @param proc Class processor (class name consumer).
     * @throws IOException In case of error.
     */
    public static void processResource(URL url, Consumer<String> proc) throws IOException {
        try (BufferedReader rdr = new BufferedReader(new InputStreamReader(url.openStream()))) {
            String line;

            while ((line = rdr.readLine()) != null) {
                if (line.isEmpty() || line.startsWith("#"))
                    continue;

                proc.accept(line.trim());
            }
        }
    }
}
