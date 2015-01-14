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

package org.gridgain.grid.kernal.processors.ggfs;

import org.apache.ignite.fs.*;

import java.io.*;
import java.lang.reflect.*;
import java.util.*;

import static org.apache.ignite.fs.IgniteFsMode.*;

/**
 * {@link GridGgfsAttributes} test case.
 */
public class GridGgfsAttributesSelfTest extends GridGgfsCommonAbstractTest {
    /**
     * @throws Exception If failed.
     */
    public void testSerialization() throws Exception {
        Map<String, IgniteFsMode> pathModes = new HashMap<>();

        pathModes.put("path1", PRIMARY);
        pathModes.put("path2", PROXY);

        GridGgfsAttributes attrs = new GridGgfsAttributes("testGgfsName", 513000, 888, "meta", "data", DUAL_SYNC,
            pathModes, true);

        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        ObjectOutput os = new ObjectOutputStream(bos);

        os.writeObject(attrs);
        os.close();

        GridGgfsAttributes deserializedAttrs = (GridGgfsAttributes)new ObjectInputStream(
            new ByteArrayInputStream(bos.toByteArray())).readObject();

        assertTrue(eq(attrs, deserializedAttrs));
    }

    /**
     * @param attr1 Attributes 1.
     * @param attr2 Attributes 2.
     * @return Whether equals or not.
     * @throws Exception In case of error.
     */
    private boolean eq(GridGgfsAttributes attr1, GridGgfsAttributes attr2) throws Exception {
        assert attr1 != null;
        assert attr2 != null;

        for (Field f : GridGgfsAttributes.class.getDeclaredFields()) {
            f.setAccessible(true);

            if (!Modifier.isStatic(f.getModifiers()) && !f.get(attr1).equals(f.get(attr2)))
                return false;
        }

        return true;
    }
}
