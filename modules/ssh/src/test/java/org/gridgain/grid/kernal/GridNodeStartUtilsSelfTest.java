/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal;

import org.apache.ignite.lang.*;
import org.gridgain.grid.util.nodestart.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.gridgain.testframework.junits.common.*;

import java.io.*;
import java.util.*;

import static org.gridgain.grid.util.nodestart.GridNodeStartUtils.*;

/**
 * Tests for {@link GridNodeStartUtils}.
 */
public class GridNodeStartUtilsSelfTest extends GridCommonAbstractTest {
    /**
     * @throws Exception If failed.
     */
    public void testParseFile() throws Exception {
        File file = U.resolveGridGainPath("modules/core/src/test/config/start-nodes.ini");

        IgniteBiTuple<Collection<Map<String, Object>>, Map<String, Object>> t = parseFile(file);

        assert t != null;

        Collection<Map<String, Object>> hosts = t.get1();

        assert hosts != null;
        assert hosts.size() == 2;

        for (Map<String, Object> host : hosts) {
            assert host != null;

            assert "192.168.1.1".equals(host.get(HOST)) || "192.168.1.2".equals(host.get(HOST));

            if ("192.168.1.1".equals(host.get(HOST))) {
                assert (Integer)host.get(PORT) == 1;
                assert "uname1".equals(host.get(UNAME));
                assert "passwd1".equals(host.get(PASSWD));
                assert new File("key1").equals(host.get(KEY));
                assert (Integer)host.get(NODES) == 1;
                assert "ggHome1".equals(host.get(GG_HOME));
                assert "cfg1".equals(host.get(CFG));
                assert "script1".equals(host.get(SCRIPT));
            }
            else if ("192.168.1.2".equals(host.get(HOST))) {
                assert (Integer)host.get(PORT) == 2;
                assert "uname2".equals(host.get(UNAME));
                assert "passwd2".equals(host.get(PASSWD));
                assert new File("key2").equals(host.get(KEY));
                assert (Integer)host.get(NODES) == 2;
                assert "ggHome2".equals(host.get(GG_HOME));
                assert "cfg2".equals(host.get(CFG));
                assert "script2".equals(host.get(SCRIPT));
            }
        }

        Map<String, Object> dflts = t.get2();

        assert dflts != null;

        assert (Integer)dflts.get(PORT) == 3;
        assert "uname3".equals(dflts.get(UNAME));
        assert "passwd3".equals(dflts.get(PASSWD));
        assert new File("key3").equals(dflts.get(KEY));
        assert (Integer)dflts.get(NODES) == 3;
        assert "ggHome3".equals(dflts.get(GG_HOME));
        assert "cfg3".equals(dflts.get(CFG));
        assert "script3".equals(dflts.get(SCRIPT));
    }
}
