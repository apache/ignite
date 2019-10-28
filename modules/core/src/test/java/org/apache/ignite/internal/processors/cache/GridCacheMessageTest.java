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

package org.apache.ignite.internal.processors.cache;

import java.io.IOException;
import java.lang.reflect.Modifier;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;
import org.springframework.core.io.Resource;
import org.springframework.core.io.support.PathMatchingResourcePatternResolver;

/**
 *
 */
public class GridCacheMessageTest extends GridCommonAbstractTest {
    /** */
    private static final String LOCATION_PATTERN_TPL = "classpath*:{package}/**/*.class";

    /** */
    private static final String LOCATION_ROOT_PACKAGE = GridCacheMessage.class.getPackage().getName()
        .replace('.', '/');

    /** */
    private static final String SUBCLASS_LOCATION_PATTERN = LOCATION_PATTERN_TPL
        .replace("{package}", LOCATION_ROOT_PACKAGE);

    /** */
    private final PathMatchingResourcePatternResolver rslvr = new PathMatchingResourcePatternResolver();

    /**
     *
     */
    @Test
    public void testDescendersToString() throws Exception {
        Set<Class<?>> abstractSubClasses = new HashSet<>();

        List<Class<?>> subClasses = Arrays.stream(rslvr.getResources(SUBCLASS_LOCATION_PATTERN))
            .map(this::toClass)
            .filter(Objects::nonNull)
            .filter(GridCacheMessage.class::isAssignableFrom)
            .filter(c -> !Modifier.isAbstract(c.getModifiers()) || !abstractSubClasses.add(c))
            .collect(Collectors.toList());

        for (Class<?> cls : subClasses) {
            abstractSubClasses.removeIf(c -> c.isAssignableFrom(cls));

            if (abstractSubClasses.isEmpty())
                break;
        }

        if (!abstractSubClasses.isEmpty())
            U.warn(log, "non-extended abstract classes found: " + abstractSubClasses.size() + " " + abstractSubClasses);

        for (Class<?> cls : subClasses)
            assertTrue(cls.newInstance().toString().contains("super=GridCacheMessage ["));
    }

    /**
     *
     */
    private Class<?> toClass(Resource rsrc) {
        try {
            String uriStr = rsrc.getURI().toString();

            if (uriStr.contains("test-classes"))
                return null;

            String clsName = uriStr.substring(
                uriStr.indexOf("org/apache/ignite"),
                uriStr.length() - 6 // Truncate the ".class"
            ).replace('/', '.');

            return Class.forName(clsName);
        }
        catch (IOException | ClassNotFoundException e) {
            throw new IllegalStateException(e);
        }
    }
}
