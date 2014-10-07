/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.ggfs;

import org.gridgain.grid.ggfs.*;

import java.io.*;
import java.lang.reflect.*;
import java.util.*;

import static org.gridgain.grid.ggfs.GridGgfsMode.*;

/**
 * {@link GridGgfsAttributes} test case.
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
