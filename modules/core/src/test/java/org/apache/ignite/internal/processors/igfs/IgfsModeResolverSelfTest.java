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

package org.apache.ignite.internal.processors.igfs;

import java.util.Arrays;
import java.util.HashSet;
import junit.framework.TestCase;
import org.apache.ignite.igfs.IgfsMode;
import org.apache.ignite.igfs.IgfsPath;
import org.apache.ignite.internal.util.typedef.T2;

import static org.apache.ignite.igfs.IgfsMode.DUAL_SYNC;
import static org.apache.ignite.igfs.IgfsMode.PRIMARY;
import static org.apache.ignite.igfs.IgfsMode.PROXY;

/**
 *
 */
public class IgfsModeResolverSelfTest extends TestCase {
    /** */
    private IgfsModeResolver resolver;

    /** {@inheritDoc} */
    @Override protected void setUp() throws Exception {
        resolver = new IgfsModeResolver(DUAL_SYNC, Arrays.asList(
            new T2<>(new IgfsPath("/a/b/"), PRIMARY),
            new T2<>(new IgfsPath("/a/b/c/d"), PROXY)));
    }

    /**
     * @throws Exception If failed.
     */
    public void testResolve() throws Exception {
        assertEquals(DUAL_SYNC, resolver.resolveMode(new IgfsPath("/")));
        assertEquals(DUAL_SYNC, resolver.resolveMode(new IgfsPath("/a")));
        assertEquals(DUAL_SYNC, resolver.resolveMode(new IgfsPath("/a/1")));
        assertEquals(PRIMARY, resolver.resolveMode(new IgfsPath("/a/b")));
        assertEquals(PRIMARY, resolver.resolveMode(new IgfsPath("/a/b/c")));
        assertEquals(PRIMARY, resolver.resolveMode(new IgfsPath("/a/b/c/2")));
        assertEquals(PROXY, resolver.resolveMode(new IgfsPath("/a/b/c/d")));
        assertEquals(PROXY, resolver.resolveMode(new IgfsPath("/a/b/c/d/e")));
    }

    /**
     * @throws Exception If failed.
     */
    public void testResolveChildren() throws Exception {
        assertEquals(new HashSet<IgfsMode>(){{add(DUAL_SYNC); add(PRIMARY); add(PROXY);}},
            resolver.resolveChildrenModes(new IgfsPath("/")));
        assertEquals(new HashSet<IgfsMode>(){{add(DUAL_SYNC); add(PRIMARY); add(PROXY);}},
            resolver.resolveChildrenModes(new IgfsPath("/a")));
        assertEquals(new HashSet<IgfsMode>(){{add(DUAL_SYNC);}},
            resolver.resolveChildrenModes(new IgfsPath("/a/1")));
        assertEquals(new HashSet<IgfsMode>(){{add(PRIMARY); add(PROXY);}},
            resolver.resolveChildrenModes(new IgfsPath("/a/b")));
        assertEquals(new HashSet<IgfsMode>(){{add(PRIMARY); add(PROXY);}},
            resolver.resolveChildrenModes(new IgfsPath("/a/b/c")));
        assertEquals(new HashSet<IgfsMode>(){{add(PRIMARY);}},
            resolver.resolveChildrenModes(new IgfsPath("/a/b/c/2")));
        assertEquals(new HashSet<IgfsMode>(){{add(PROXY);}},
            resolver.resolveChildrenModes(new IgfsPath("/a/b/c/d")));
        assertEquals(new HashSet<IgfsMode>(){{add(PROXY);}},
            resolver.resolveChildrenModes(new IgfsPath("/a/b/c/d/e")));
    }
}