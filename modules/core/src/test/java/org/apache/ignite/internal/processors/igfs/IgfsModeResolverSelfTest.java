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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import junit.framework.TestCase;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.igfs.IgfsMode;
import org.apache.ignite.igfs.IgfsPath;
import org.apache.ignite.internal.util.typedef.T2;
import static org.apache.ignite.igfs.IgfsMode.DUAL_ASYNC;
import static org.apache.ignite.igfs.IgfsMode.DUAL_SYNC;
import static org.apache.ignite.igfs.IgfsMode.PRIMARY;
import static org.apache.ignite.igfs.IgfsMode.PROXY;

/**
 *
 */
public class IgfsModeResolverSelfTest extends TestCase {
    /** */
    private IgfsModeResolver reslvr;

    /** {@inheritDoc} */
    @Override protected void setUp() throws Exception {
        reslvr = new IgfsModeResolver(DUAL_SYNC, new ArrayList<>(Arrays.asList(new T2<>(
            new IgfsPath("/a/b/c/d"), PROXY), new T2<>(new IgfsPath("/a/P/"), PRIMARY), new T2<>(new IgfsPath("/a/b/"),
            DUAL_ASYNC))));
    }

    /**
     * @throws Exception If failed.
     */
    public void testCanContain() throws Exception {
        for (IgfsMode m: IgfsMode.values()) {
            // Each mode can contain itself:
            assertTrue(IgfsUtils.canContain(m, m));

            // PRIMARY and PROXY can contain itself only:
            assertTrue(IgfsUtils.canContain(PRIMARY,m) == (m == PRIMARY));
            assertTrue(IgfsUtils.canContain(PROXY,m) == (m == PROXY));

            // Any mode but PRIMARY & PROXY can contain any mode:
            if (m != PRIMARY && m != PROXY)
                for (IgfsMode n: IgfsMode.values())
                    assertTrue(IgfsUtils.canContain(m,n));
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testResolve() throws Exception {
        assertEquals(DUAL_SYNC, reslvr.resolveMode(IgfsPath.ROOT));
        assertEquals(DUAL_SYNC, reslvr.resolveMode(new IgfsPath("/a")));
        assertEquals(DUAL_SYNC, reslvr.resolveMode(new IgfsPath("/a/1")));

        assertEquals(PRIMARY, reslvr.resolveMode(new IgfsPath("/a/P")));
        assertEquals(PRIMARY, reslvr.resolveMode(new IgfsPath("/a/P/c")));
        assertEquals(PRIMARY, reslvr.resolveMode(new IgfsPath("/a/P/c/2")));

        assertEquals(DUAL_ASYNC, reslvr.resolveMode(new IgfsPath("/a/b/")));
        assertEquals(DUAL_ASYNC, reslvr.resolveMode(new IgfsPath("/a/b/3")));
        assertEquals(DUAL_ASYNC, reslvr.resolveMode(new IgfsPath("/a/b/3/4")));

        assertEquals(PROXY, reslvr.resolveMode(new IgfsPath("/a/b/c/d")));
        assertEquals(PROXY, reslvr.resolveMode(new IgfsPath("/a/b/c/d/5")));
        assertEquals(PROXY, reslvr.resolveMode(new IgfsPath("/a/b/c/d/6")));
    }

    /**
     * @throws Exception If failed.
     */
    public void testModesValidation() throws Exception {
        // Another mode inside PRIMARY directory:
        try {
            IgfsUtils.preparePathModes(DUAL_SYNC, Arrays.asList(
                new T2<>(new IgfsPath("/a/"), PRIMARY),
                new T2<>(new IgfsPath("/a/b/"), DUAL_ASYNC)), new HashSet<IgfsPath>());

            fail("IgniteCheckedException expected");
        }
        catch (IgniteCheckedException ignored) {
            // No-op.
        }

        // PRIMARY default mode and non-primary subfolder:
        for (IgfsMode m: IgfsMode.values()) {
            if (m != IgfsMode.PRIMARY) {
                try {
                    IgfsUtils.preparePathModes(PRIMARY, Arrays.asList(new T2<>(new IgfsPath("/a/"), DUAL_ASYNC)),
                        new HashSet<IgfsPath>());

                    fail("IgniteCheckedException expected");
                }
                catch (IgniteCheckedException ignored) {
                    // No-op.
                }
            }
        }

        // Duplicated sub-folders should be ignored:
        List<T2<IgfsPath, IgfsMode>> modes = IgfsUtils.preparePathModes(DUAL_SYNC, Arrays.asList(
            new T2<>(new IgfsPath("/a"), PRIMARY),
            new T2<>(new IgfsPath("/c/d/"), PRIMARY),
            new T2<>(new IgfsPath("/c/d/e/f"), PRIMARY)
        ), new HashSet<IgfsPath>());
        assertNotNull(modes);
        assertEquals(2, modes.size());
        assertEquals(modes, Arrays.asList(
            new T2<>(new IgfsPath("/c/d/"), PRIMARY),
            new T2<>(new IgfsPath("/a"), PRIMARY)
        ));

        // Non-duplicated sub-folders should not be ignored:
        modes = IgfsUtils.preparePathModes(DUAL_SYNC, Arrays.asList(
            new T2<>(new IgfsPath("/a/b"), DUAL_ASYNC),
            new T2<>(new IgfsPath("/a/b/c"), DUAL_SYNC),
            new T2<>(new IgfsPath("/a/b/c/d"), DUAL_ASYNC)
        ), new HashSet<IgfsPath>());
        assertNotNull(modes);
        assertEquals(modes.size(), 3);
        assertEquals(modes, Arrays.asList(
            new T2<>(new IgfsPath("/a/b/c/d"), DUAL_ASYNC),
            new T2<>(new IgfsPath("/a/b/c"), DUAL_SYNC),
            new T2<>(new IgfsPath("/a/b"), DUAL_ASYNC)
        ));
    }

    /**
     * @throws Exception If failed.
     */
    public void testDualParentsWithPrimaryChild() throws Exception {
        Set<IgfsPath> set = new HashSet<>();

        IgfsUtils.preparePathModes(DUAL_SYNC, Arrays.asList(
            new T2<>(new IgfsPath("/a/b"), DUAL_ASYNC),
            new T2<>(new IgfsPath("/a/b/c"), PRIMARY),
            new T2<>(new IgfsPath("/a/b/x/y"), PRIMARY),
            new T2<>(new IgfsPath("/a/b/x/z"), PRIMARY),
            new T2<>(new IgfsPath("/m"), PRIMARY)
        ), set);
        assertEquals(set, new HashSet<IgfsPath>() {{
            add(new IgfsPath("/a/b"));
            add(new IgfsPath("/a/b/x"));
            add(IgfsPath.ROOT);
        }});

        set = new HashSet<>();

        IgfsUtils.preparePathModes(DUAL_ASYNC, Arrays.asList(
            new T2<>(new IgfsPath("/a/b/x/y/z"), PRIMARY),
            new T2<>(new IgfsPath("/a/b/c"), PRIMARY),
            new T2<>(new IgfsPath("/a/k"), PRIMARY),
            new T2<>(new IgfsPath("/a/z"), PRIMARY)
        ), set);
        assertEquals(set, new HashSet<IgfsPath>() {{
            add(new IgfsPath("/a/b"));
            add(new IgfsPath("/a"));
            add(new IgfsPath("/a/b/x/y"));
        }});
    }
}
