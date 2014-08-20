/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.ggfs;

import org.apache.commons.lang.builder.*;
import org.gridgain.grid.ggfs.*;

import java.io.*;
import java.util.*;

import static org.gridgain.grid.ggfs.GridGgfsMode.*;

/**
 * {@link org.gridgain.grid.kernal.processors.ggfs.GridGgfsAttributes} test case.
 */
public class GridGgfsAttributesSelfTest extends GridGgfsCommonAbstractTest {
    /**
     * @throws Exception If failed.
     */
    public void testSerialization() throws Exception {
        Map<String, GridGgfsMode> pathModes = new HashMap<>();

        pathModes.put("path1", PRIMARY);
        pathModes.put("path2", PROXY);

        GridGgfsAttributes attrs = new GridGgfsAttributes("testGgfsName", 513000, 888, "meta", "data", DUAL_SYNC,
            pathModes, true);

        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        ObjectOutputStream os = new ObjectOutputStream(bos);
        os.writeObject(attrs);
        os.close();

        GridGgfsAttributes deserializedAttrs = (GridGgfsAttributes)new ObjectInputStream(
            new ByteArrayInputStream(bos.toByteArray())).readObject();

        assertTrue(EqualsBuilder.reflectionEquals(attrs, deserializedAttrs));
    }
}
