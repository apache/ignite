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

package org.apache.ignite.internal;

import java.io.File;
import java.util.Collection;
import java.util.Map;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

import static org.apache.ignite.internal.util.nodestart.IgniteNodeStartUtils.CFG;
import static org.apache.ignite.internal.util.nodestart.IgniteNodeStartUtils.HOST;
import static org.apache.ignite.internal.util.nodestart.IgniteNodeStartUtils.IGNITE_HOME;
import static org.apache.ignite.internal.util.nodestart.IgniteNodeStartUtils.KEY;
import static org.apache.ignite.internal.util.nodestart.IgniteNodeStartUtils.NODES;
import static org.apache.ignite.internal.util.nodestart.IgniteNodeStartUtils.PASSWD;
import static org.apache.ignite.internal.util.nodestart.IgniteNodeStartUtils.PORT;
import static org.apache.ignite.internal.util.nodestart.IgniteNodeStartUtils.SCRIPT;
import static org.apache.ignite.internal.util.nodestart.IgniteNodeStartUtils.UNAME;
import static org.apache.ignite.internal.util.nodestart.IgniteNodeStartUtils.parseFile;

/**
 * Tests for {@link org.apache.ignite.internal.util.nodestart.IgniteNodeStartUtils}.
 */
public class IgniteNodeStartUtilsSelfTest extends GridCommonAbstractTest {
    /**
     * @throws Exception If failed.
     */
    public void testParseFile() throws Exception {
        File file = U.resolveIgnitePath("modules/core/src/test/config/start-nodes.ini");

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
                assert "ggHome1".equals(host.get(IGNITE_HOME));
                assert "cfg1".equals(host.get(CFG));
                assert "script1".equals(host.get(SCRIPT));
            }
            else if ("192.168.1.2".equals(host.get(HOST))) {
                assert (Integer)host.get(PORT) == 2;
                assert "uname2".equals(host.get(UNAME));
                assert "passwd2".equals(host.get(PASSWD));
                assert new File("key2").equals(host.get(KEY));
                assert (Integer)host.get(NODES) == 2;
                assert "ggHome2".equals(host.get(IGNITE_HOME));
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
        assert "ggHome3".equals(dflts.get(IGNITE_HOME));
        assert "cfg3".equals(dflts.get(CFG));
        assert "script3".equals(dflts.get(SCRIPT));
    }
}