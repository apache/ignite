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

import junit.framework.*;
import org.apache.ignite.fs.*;
import org.gridgain.grid.util.typedef.*;

import java.util.*;

import static org.apache.ignite.fs.IgniteFsMode.*;

/**
 *
 */
public class GridGgfsModeResolverSelfTest extends TestCase {
    /** */
    private GridGgfsModeResolver resolver;

    /** {@inheritDoc} */
    @Override protected void setUp() throws Exception {
        resolver = new GridGgfsModeResolver(DUAL_SYNC, Arrays.asList(
            new T2<>(new IgniteFsPath("/a/b/"), PRIMARY),
            new T2<>(new IgniteFsPath("/a/b/c/d"), PROXY)));
    }

    /**
     * @throws Exception If failed.
     */
    public void testResolve() throws Exception {
        assertEquals(DUAL_SYNC, resolver.resolveMode(new IgniteFsPath("/")));
        assertEquals(DUAL_SYNC, resolver.resolveMode(new IgniteFsPath("/a")));
        assertEquals(DUAL_SYNC, resolver.resolveMode(new IgniteFsPath("/a/1")));
        assertEquals(PRIMARY, resolver.resolveMode(new IgniteFsPath("/a/b")));
        assertEquals(PRIMARY, resolver.resolveMode(new IgniteFsPath("/a/b/c")));
        assertEquals(PRIMARY, resolver.resolveMode(new IgniteFsPath("/a/b/c/2")));
        assertEquals(PROXY, resolver.resolveMode(new IgniteFsPath("/a/b/c/d")));
        assertEquals(PROXY, resolver.resolveMode(new IgniteFsPath("/a/b/c/d/e")));
    }

    /**
     * @throws Exception If failed.
     */
    public void testResolveChildren() throws Exception {
        assertEquals(new HashSet<IgniteFsMode>(){{add(DUAL_SYNC); add(PRIMARY); add(PROXY);}},
            resolver.resolveChildrenModes(new IgniteFsPath("/")));
        assertEquals(new HashSet<IgniteFsMode>(){{add(DUAL_SYNC); add(PRIMARY); add(PROXY);}},
            resolver.resolveChildrenModes(new IgniteFsPath("/a")));
        assertEquals(new HashSet<IgniteFsMode>(){{add(DUAL_SYNC);}},
            resolver.resolveChildrenModes(new IgniteFsPath("/a/1")));
        assertEquals(new HashSet<IgniteFsMode>(){{add(PRIMARY); add(PROXY);}},
            resolver.resolveChildrenModes(new IgniteFsPath("/a/b")));
        assertEquals(new HashSet<IgniteFsMode>(){{add(PRIMARY); add(PROXY);}},
            resolver.resolveChildrenModes(new IgniteFsPath("/a/b/c")));
        assertEquals(new HashSet<IgniteFsMode>(){{add(PRIMARY);}},
            resolver.resolveChildrenModes(new IgniteFsPath("/a/b/c/2")));
        assertEquals(new HashSet<IgniteFsMode>(){{add(PROXY);}},
            resolver.resolveChildrenModes(new IgniteFsPath("/a/b/c/d")));
        assertEquals(new HashSet<IgniteFsMode>(){{add(PROXY);}},
            resolver.resolveChildrenModes(new IgniteFsPath("/a/b/c/d/e")));
    }
}
