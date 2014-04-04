/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.util;

import org.gridgain.grid.util.typedef.*;
import org.gridgain.testframework.junits.common.*;

/**
 * Grid utils tests.
 */
@GridCommonTest(group = "Utils")
public class GridQueueSelfTest extends GridCommonAbstractTest {
    /**
     *
     */
    public void testQueue() {
        GridQueue<String> q = new GridQueue<>();
        for (char c = 'a'; c <= 'z'; c++)
            q.offer(Character.toString(c));

        assertEquals('z' - 'a' + 1, q.size());

        char ch = 'a';

        for (String c = q.poll(); c != null; c = q.poll()) {
            X.println(c);

            assertEquals(Character.toString(ch++), c);
        }

        assert q.isEmpty();

        for (char c = 'A'; c <= 'Z'; c++)
            q.offer(Character.toString(c));

        assertEquals('Z' - 'A' + 1, q.size());

        ch = 'A';

        for (String s : q) {
            X.println(s);

            assertEquals(Character.toString(ch++), s);
        }

        q.remove("O");

        assertEquals('Z' - 'A', q.size());

        for (String c = q.poll(); c != null; c = q.poll())
            assert !"O".equals(c);

        assert q.isEmpty();
    }
}
