/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.ggfs;

import junit.framework.*;
import org.gridgain.grid.ggfs.*;
import org.gridgain.grid.util.typedef.*;

import java.util.*;

import static org.gridgain.grid.ggfs.IgniteFsMode.*;

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
