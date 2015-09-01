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
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCluster;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.cluster.ClusterStartNodeResult;
import org.apache.ignite.events.Event;
import org.apache.ignite.internal.util.nodestart.IgniteNodeStartUtils;
import org.apache.ignite.internal.util.typedef.CI1;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteClosure;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.jetbrains.annotations.Nullable;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.apache.ignite.events.EventType.EVT_NODE_FAILED;
import static org.apache.ignite.events.EventType.EVT_NODE_JOINED;
import static org.apache.ignite.events.EventType.EVT_NODE_LEFT;
import static org.apache.ignite.internal.util.nodestart.IgniteNodeStartUtils.CFG;
import static org.apache.ignite.internal.util.nodestart.IgniteNodeStartUtils.DFLT_TIMEOUT;
import static org.apache.ignite.internal.util.nodestart.IgniteNodeStartUtils.IGNITE_HOME;
import static org.apache.ignite.internal.util.nodestart.IgniteNodeStartUtils.KEY;
import static org.apache.ignite.internal.util.nodestart.IgniteNodeStartUtils.NODES;
import static org.apache.ignite.internal.util.nodestart.IgniteNodeStartUtils.PASSWD;
import static org.apache.ignite.internal.util.nodestart.IgniteNodeStartUtils.SCRIPT;
import static org.apache.ignite.internal.util.nodestart.IgniteNodeStartUtils.UNAME;

/**
 * Tests for {@code startNodes(..)}, {@code stopNodes(..)}
 * and {@code restartNodes(..)} methods.
 * <p>
 * {@code tests.properties} file must specify username ({@code ssh.username} property)
 * and one (and only one) of password ({@code ssh.password} property) or
 * private key path ({@code ssh.key} property).
 */
@SuppressWarnings("ConstantConditions")
public class IgniteProjectionStartStopRestartSelfTest extends GridCommonAbstractTest {
    /** */
    private static final String SSH_UNAME = System.getenv("test.ssh.username");

    /** */
    private static final String SSH_PWD = System.getenv("test.ssh.password");

    /** */
    private static final String SSH_KEY = System.getenv("ssh.key");

    /** */
    private static final String CUSTOM_SCRIPT_WIN = "modules/core/src/test/bin/start-nodes-custom.bat";

    /** */
    private static final String CUSTOM_SCRIPT_LINUX = "modules/core/src/test/bin/start-nodes-custom.sh";

    /** */
    private static final String CFG_NO_ATTR = "modules/core/src/test/config/spring-start-nodes.xml";

    /** */
    private static final String CFG_ATTR = "modules/core/src/test/config/spring-start-nodes-attr.xml";

    /** */
    private static final String CUSTOM_CFG_ATTR_KEY = "grid.node.ssh.started";

    /** */
    private static final String CUSTOM_CFG_ATTR_VAL = "true";

    /** */
    private static final long WAIT_TIMEOUT = 40 * 1000;

    /** */
    private String pwd;

    /** */
    private File key;

    /** */
    private Ignite ignite;

    /** */
    private static final String HOST = "127.0.0.1";

    /** */
    private final AtomicInteger joinedCnt = new AtomicInteger();

    /** */
    private final AtomicInteger leftCnt = new AtomicInteger();

    /** */
    private volatile CountDownLatch joinedLatch;

    /** */
    private volatile CountDownLatch leftLatch;

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        if (SSH_KEY != null) {
            key = new File(SSH_KEY);

            assert key.exists() : "Private key doesn't exist: " + key.getAbsolutePath();
            assert key.isFile() : "Private key is not a file: " + key.getAbsolutePath();
        }
        else
            pwd = SSH_PWD;

        log.info("Username: " + SSH_UNAME);
        log.info("Password: " + pwd);
        log.info("Key path: " + key);

        G.setDaemon(true);

        ignite = G.start(CFG_NO_ATTR);

        G.setDaemon(false);

        ignite.events().localListen(new IgnitePredicate<Event>() {
            @Override public boolean apply(Event evt) {
                info("Received event: " + evt.shortDisplay());

                if (evt.type() == EVT_NODE_JOINED) {
                    joinedCnt.incrementAndGet();

                    if (joinedLatch != null)
                        joinedLatch.countDown();
                }
                else if (evt.type() == EVT_NODE_LEFT || evt.type() == EVT_NODE_FAILED) {
                    leftCnt.incrementAndGet();

                    if (leftLatch != null)
                        leftLatch.countDown();
                }

                return true;
            }
        }, EVT_NODE_JOINED, EVT_NODE_LEFT, EVT_NODE_FAILED);
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        boolean wasEmpty = true;

        if (ignite != null) {
            if (!ignite.cluster().nodes().isEmpty()) {
                leftLatch = new CountDownLatch(ignite.cluster().nodes().size());

                ignite.cluster().stopNodes();

                assert leftLatch.await(
                    WAIT_TIMEOUT,
                    MILLISECONDS);
            }

            wasEmpty = ignite.cluster().nodes().isEmpty();
        }

        G.stop(true);

        joinedCnt.set(0);
        leftCnt.set(0);

        joinedLatch = null;
        leftLatch = null;

        assert wasEmpty : "grid.isEmpty() returned false after all nodes were stopped " +
            "[nodes=" + ignite.cluster().nodes() + ']';
    }

    /** {@inheritDoc} */
    @Override protected long getTestTimeout() {
        return 90 * 1000;
    }

    /**
     * @throws Exception If failed.
     */
    public void testStartOneNode() throws Exception {
        joinedLatch = new CountDownLatch(1);

        Collection<ClusterStartNodeResult> res =
            startNodes(ignite.cluster(),
                maps(Collections.singleton(HOST), SSH_UNAME, pwd, key, 1, U.getIgniteHome(), CFG_NO_ATTR, null),
                null, false, 0, 16);

        assert res.size() == 1;

        F.forEach(res, new CI1<ClusterStartNodeResult>() {
            @Override public void apply(ClusterStartNodeResult t) {
                assert t.getHostName().equals(HOST);

                if (!t.isSuccess())
                    throw new IgniteException(t.getError());
            }
        });

        assert joinedLatch.await(WAIT_TIMEOUT, MILLISECONDS);

        assert joinedCnt.get() == 1;
        assert leftCnt.get() == 0;

        assert ignite.cluster().nodes().size() == 1;
    }

    /**
     * @throws Exception If failed.
     */
    public void testStartThreeNodes() throws Exception {
        joinedLatch = new CountDownLatch(3);

        Collection<ClusterStartNodeResult> res =
            startNodes(ignite.cluster(),
                maps(Collections.singleton(HOST), SSH_UNAME, pwd, key, 3, U.getIgniteHome(), CFG_NO_ATTR, null),
                null, false, DFLT_TIMEOUT, 1);

        assert res.size() == 3;

        F.forEach(res, new CI1<ClusterStartNodeResult>() {
            @Override public void apply(ClusterStartNodeResult t) {
                assert t.getHostName().equals(HOST);

                if (!t.isSuccess())
                    throw new IgniteException(t.getError());
            }
        });

        assert joinedLatch.await(WAIT_TIMEOUT, MILLISECONDS);

        assert joinedCnt.get() == 3;
        assert leftCnt.get() == 0;

        assert ignite.cluster().nodes().size() == 3;
    }

    /**
     * @throws Exception If failed.
     */
    public void testStartThreeNodesAndDoEmptyCall() throws Exception {
        joinedLatch = new CountDownLatch(3);

        Collection<ClusterStartNodeResult> res =
            startNodes(ignite.cluster(),
                maps(Collections.singleton(HOST), SSH_UNAME, pwd, key, 3, U.getIgniteHome(), CFG_NO_ATTR, null),
                null, false, 0, 16);

        assert res.size() == 3;

        F.forEach(res, new CI1<ClusterStartNodeResult>() {
            @Override public void apply(ClusterStartNodeResult t) {
                assert t.getHostName().equals(HOST);

                if (!t.isSuccess())
                    throw new IgniteException(t.getError());
            }
        });

        assert joinedLatch.await(WAIT_TIMEOUT, MILLISECONDS);

        assert joinedCnt.get() == 3;
        assert leftCnt.get() == 0;

        assert ignite.cluster().nodes().size() == 3;

        res = startNodes(ignite.cluster(),
            maps(Collections.singleton(HOST), SSH_UNAME, pwd, key, 3, U.getIgniteHome(), CFG_NO_ATTR, null),
            null, false, 0, 16);

        assert res.isEmpty();

        assert joinedCnt.get() == 3;
        assert leftCnt.get() == 0;

        assert ignite.cluster().nodes().size() == 3;
    }

    /**
     * @throws Exception If failed.
     */
    public void testStartThreeNodesAndTryToStartOneNode() throws Exception {
        joinedLatch = new CountDownLatch(3);

        Collection<ClusterStartNodeResult> res =
            startNodes(ignite.cluster(),
                maps(Collections.singleton(HOST), SSH_UNAME, pwd, key, 3, U.getIgniteHome(), CFG_NO_ATTR, null),
                null, false, 0, 16);

        assert res.size() == 3;

        F.forEach(res, new CI1<ClusterStartNodeResult>() {
            @Override public void apply(ClusterStartNodeResult t) {
                assert t.getHostName().equals(HOST);

                if (!t.isSuccess())
                    throw new IgniteException(t.getError());
            }
        });

        assert joinedLatch.await(WAIT_TIMEOUT, MILLISECONDS);

        assert joinedCnt.get() == 3;
        assert leftCnt.get() == 0;

        assert ignite.cluster().nodes().size() == 3;

        res = startNodes(ignite.cluster(),
            maps(Collections.singleton(HOST), SSH_UNAME, pwd, key, 1, U.getIgniteHome(), CFG_NO_ATTR, null),
            null, false, 0, 16);

        assert res.isEmpty();

        assert joinedCnt.get() == 3;
        assert leftCnt.get() == 0;

        assert ignite.cluster().nodes().size() == 3;
    }

    /**
     * @throws Exception If failed.
     */
    public void testStartFiveNodesInTwoCalls() throws Exception {
        joinedLatch = new CountDownLatch(3);

        Collection<ClusterStartNodeResult> res =
            startNodes(ignite.cluster(),
                maps(Collections.singleton(HOST), SSH_UNAME, pwd, key, 3, U.getIgniteHome(), CFG_NO_ATTR, null),
                null, false, 0, 16);

        assert res.size() == 3;

        F.forEach(res, new CI1<ClusterStartNodeResult>() {
            @Override public void apply(ClusterStartNodeResult t) {
                assert t.getHostName().equals(HOST);

                if (!t.isSuccess())
                    throw new IgniteException(t.getError());
            }
        });

        assert joinedLatch.await(WAIT_TIMEOUT, MILLISECONDS);

        assert joinedCnt.get() == 3;
        assert leftCnt.get() == 0;

        assert ignite.cluster().nodes().size() == 3;

        joinedLatch = new CountDownLatch(2);

        res = startNodes(ignite.cluster(),
            maps(Collections.singleton(HOST), SSH_UNAME, pwd, key, 5, U.getIgniteHome(), CFG_NO_ATTR, null),
            null, false, 0, 16);

        assert res.size() == 2;

        F.forEach(res, new CI1<ClusterStartNodeResult>() {
            @Override public void apply(ClusterStartNodeResult t) {
                assert t.getHostName().equals(HOST);

                if (!t.isSuccess())
                    throw new IgniteException(t.getError());
            }
        });

        assert joinedLatch.await(WAIT_TIMEOUT, MILLISECONDS);

        assert joinedCnt.get() == 5;
        assert leftCnt.get() == 0;

        assert ignite.cluster().nodes().size() == 5;
    }

    /**
     * @throws Exception If failed.
     */
    public void testStartFiveWithTwoSpecs() throws Exception {
        joinedLatch = new CountDownLatch(5);

        Collection<ClusterStartNodeResult> res =
            startNodes(ignite.cluster(),
                F.asList(map(HOST, SSH_UNAME, pwd, key, 2, U.getIgniteHome(), CFG_NO_ATTR, null),
                    map(HOST, SSH_UNAME, pwd, key, 3, U.getIgniteHome(), CFG_NO_ATTR, null)),
                null, false, 0, 16);

        assert res.size() == 5;

        F.forEach(res, new CI1<ClusterStartNodeResult>() {
            @Override public void apply(ClusterStartNodeResult t) {
                assert t.getHostName().equals(HOST);

                if (!t.isSuccess())
                    throw new IgniteException(t.getError());
            }
        });

        assert joinedLatch.await(WAIT_TIMEOUT, MILLISECONDS);

        assert joinedCnt.get() == 5;
        assert leftCnt.get() == 0;

        assert ignite.cluster().nodes().size() == 5;
    }

    /**
     * @throws Exception If failed.
     */
    public void testStartThreeNodesAndRestart() throws Exception {
        joinedLatch = new CountDownLatch(3);

        Collection<ClusterStartNodeResult> res =
            startNodes(ignite.cluster(),
                maps(Collections.singleton(HOST), SSH_UNAME, pwd, key, 3, U.getIgniteHome(), CFG_NO_ATTR, null),
                null, false, 0, 16);

        assert res.size() == 3;

        F.forEach(res, new CI1<ClusterStartNodeResult>() {
            @Override public void apply(ClusterStartNodeResult t) {
                assert t.getHostName().equals(HOST);

                if (!t.isSuccess())
                    throw new IgniteException(t.getError());
            }
        });

        assert joinedLatch.await(WAIT_TIMEOUT, MILLISECONDS);

        assert joinedCnt.get() == 3;
        assert leftCnt.get() == 0;

        assert ignite.cluster().nodes().size() == 3;

        joinedLatch = new CountDownLatch(3);
        leftLatch = new CountDownLatch(3);

        res = startNodes(ignite.cluster(),
            maps(Collections.singleton(HOST), SSH_UNAME, pwd, key, 3, U.getIgniteHome(), CFG_NO_ATTR, null),
            null, true, 0, 16);

        assert res.size() == 3;

        F.forEach(res, new CI1<ClusterStartNodeResult>() {
            @Override public void apply(ClusterStartNodeResult t) {
                assert t.getHostName().equals(HOST);

                if (!t.isSuccess())
                    throw new IgniteException(t.getError());
            }
        });

        assert joinedLatch.await(WAIT_TIMEOUT, MILLISECONDS);
        assert leftLatch.await(WAIT_TIMEOUT, MILLISECONDS);

        assert joinedCnt.get() == 6;
        assert leftCnt.get() == 3;

        assert ignite.cluster().nodes().size() == 3;
    }

    /**
     * @throws Exception If failed.
     */
    public void testCustomScript() throws Exception {
        joinedLatch = new CountDownLatch(1);

        String script = U.isWindows() ? CUSTOM_SCRIPT_WIN : CUSTOM_SCRIPT_LINUX;

        script = Paths.get(U.getIgniteHome()).relativize(U.resolveIgnitePath(script).toPath()).toString();

        Collection<ClusterStartNodeResult> res =
            startNodes(ignite.cluster(),
                maps(Collections.singleton(HOST), SSH_UNAME, pwd, key, 1, U.getIgniteHome(), null, script),
                null, false, 0, 16);

        assert res.size() == 1;

        F.forEach(res, new CI1<ClusterStartNodeResult>() {
            @Override public void apply(ClusterStartNodeResult t) {
                assert t.getHostName().equals(HOST);

                if (!t.isSuccess())
                    throw new IgniteException(t.getError());
            }
        });

        assert joinedLatch.await(WAIT_TIMEOUT, MILLISECONDS);

        assert joinedCnt.get() == 1;
        assert leftCnt.get() == 0;

        assert ignite.cluster().nodes().size() == 1;

        assert CUSTOM_CFG_ATTR_VAL.equals(F.first(ignite.cluster().nodes()).<String>attribute(CUSTOM_CFG_ATTR_KEY));
    }

    /**
     * @throws Exception If failed.
     */
    public void testStopNodes() throws Exception {
        joinedLatch = new CountDownLatch(3);

        Collection<ClusterStartNodeResult> res =
            startNodes(ignite.cluster(),
                maps(Collections.singleton(HOST), SSH_UNAME, pwd, null, 3, U.getIgniteHome(), CFG_NO_ATTR,
                null), null, false, 0, 16);

        assert res.size() == 3;

        F.forEach(res, new CI1<ClusterStartNodeResult>() {
            @Override public void apply(ClusterStartNodeResult t) {
                assert t.getHostName().equals(HOST);

                if (!t.isSuccess())
                    throw new IgniteException(t.getError());
            }
        });

        assert joinedLatch.await(WAIT_TIMEOUT, MILLISECONDS);

        assert ignite.cluster().nodes().size() == 3;

        leftLatch = new CountDownLatch(3);

        ignite.cluster().stopNodes();

        assert leftLatch.await(WAIT_TIMEOUT, MILLISECONDS);

        assert ignite.cluster().nodes().isEmpty();
    }

    /**
     * @throws Exception If failed.
     */
    public void testStopNodesFiltered() throws Exception {
        joinedLatch = new CountDownLatch(2);

        Collection<ClusterStartNodeResult> res =
            startNodes(ignite.cluster(),
                maps(Collections.singleton(HOST), SSH_UNAME, pwd, key, 2, U.getIgniteHome(), CFG_ATTR, null),
                null, false, 0, 16);

        assert res.size() == 2;

        F.forEach(res, new CI1<ClusterStartNodeResult>() {
            @Override public void apply(ClusterStartNodeResult t) {
                assert t.getHostName().equals(HOST);

                if (!t.isSuccess())
                    throw new IgniteException(t.getError());
            }
        });

        assert joinedLatch.await(WAIT_TIMEOUT, MILLISECONDS);

        joinedLatch = new CountDownLatch(1);

        res = startNodes(ignite.cluster(),
            maps(Collections.singleton(HOST), SSH_UNAME, pwd, key, 3, U.getIgniteHome(), CFG_NO_ATTR, null),
            null, false, 0, 16);

        assert res.size() == 1;

        F.forEach(res, new CI1<ClusterStartNodeResult>() {
            @Override public void apply(ClusterStartNodeResult t) {
                assert t.getHostName().equals(HOST);

                if (!t.isSuccess())
                    throw new IgniteException(t.getError());
            }
        });

        assert joinedLatch.await(WAIT_TIMEOUT, MILLISECONDS);

        assert ignite.cluster().nodes().size() == 3;

        leftLatch = new CountDownLatch(2);

        Collection<UUID> ids = F.transform(ignite.cluster().forAttribute(CUSTOM_CFG_ATTR_KEY, CUSTOM_CFG_ATTR_VAL).nodes(),
            new IgniteClosure<ClusterNode, UUID>() {
            @Override public UUID apply(ClusterNode node) {
                return node.id();
            }
        });

        ignite.cluster().forAttribute(CUSTOM_CFG_ATTR_KEY, CUSTOM_CFG_ATTR_VAL).nodes();

        ignite.cluster().stopNodes(ids);

        assert leftLatch.await(WAIT_TIMEOUT, MILLISECONDS);

        assert ignite.cluster().nodes().size() == 1;
    }

    /**
     * @throws Exception If failed.
     */
    public void testStopNodeById() throws Exception {
        joinedLatch = new CountDownLatch(3);

        Collection<ClusterStartNodeResult> res =
            startNodes(ignite.cluster(),
                maps(Collections.singleton(HOST), SSH_UNAME, pwd, key, 3, U.getIgniteHome(), CFG_NO_ATTR, null),
                null, false, 0, 16);

        assert res.size() == 3;

        F.forEach(res, new CI1<ClusterStartNodeResult>() {
            @Override public void apply(ClusterStartNodeResult t) {
                assert t.getHostName().equals(HOST);

                if (!t.isSuccess())
                    throw new IgniteException(t.getError());
            }
        });

        assert joinedLatch.await(WAIT_TIMEOUT, MILLISECONDS);

        assert ignite.cluster().nodes().size() == 3;

        leftLatch = new CountDownLatch(1);

        ignite.cluster().stopNodes(Collections.singleton(F.first(ignite.cluster().forRemotes().nodes()).id()));

        assert leftLatch.await(WAIT_TIMEOUT, MILLISECONDS);

        assert ignite.cluster().nodes().size() == 2;
    }

    /**
     * @throws Exception If failed.
     */
    public void testStopNodesByIds() throws Exception {
        joinedLatch = new CountDownLatch(3);

        Collection<ClusterStartNodeResult> res =
            startNodes(ignite.cluster(),
                maps(Collections.singleton(HOST), SSH_UNAME, pwd, key, 3, U.getIgniteHome(), CFG_NO_ATTR, null),
                null, false, 0, 16);

        assert res.size() == 3;

        F.forEach(res, new CI1<ClusterStartNodeResult>() {
            @Override public void apply(ClusterStartNodeResult t) {
                assert t.getHostName().equals(HOST);

                if (!t.isSuccess())
                    throw new IgniteException(t.getError());
            }
        });

        assert joinedLatch.await(WAIT_TIMEOUT, MILLISECONDS);

        assert ignite.cluster().nodes().size() == 3;

        leftLatch = new CountDownLatch(2);

        Iterator<ClusterNode> it = ignite.cluster().nodes().iterator();

        Collection<UUID> ids = new HashSet<>();

        ids.add(it.next().id());
        ids.add(it.next().id());

        ignite.cluster().stopNodes(ids);

        assert leftLatch.await(WAIT_TIMEOUT, MILLISECONDS);

        assert ignite.cluster().nodes().size() == 1;
    }

    /**
     * @throws Exception If failed.
     */
    public void testStopNodesByIdsC() throws Exception {
        joinedLatch = new CountDownLatch(3);

        Collection<ClusterStartNodeResult> res =
            startNodes(ignite.cluster(),
                maps(Collections.singleton(HOST), SSH_UNAME, pwd, key, 3, U.getIgniteHome(), CFG_NO_ATTR, null),
                null, false, 0, 16);

        assert res.size() == 3;

        F.forEach(res, new CI1<ClusterStartNodeResult>() {
            @Override public void apply(ClusterStartNodeResult t) {
                assert t.getHostName().equals(HOST);

                if (!t.isSuccess())
                    throw new IgniteException(t.getError());
            }
        });

        assert joinedLatch.await(WAIT_TIMEOUT, MILLISECONDS);

        assert ignite.cluster().nodes().size() == 3;

        leftLatch = new CountDownLatch(2);

        Iterator<ClusterNode> it = ignite.cluster().nodes().iterator();

        ignite.cluster().stopNodes(F.asList(it.next().id(), it.next().id()));

        assert leftLatch.await(WAIT_TIMEOUT, MILLISECONDS);

        assert ignite.cluster().nodes().size() == 1;
    }

    /**
     * @throws Exception If failed.
     */
    public void testRestartNodes() throws Exception {
        joinedLatch = new CountDownLatch(3);

        Collection<ClusterStartNodeResult> res =
            startNodes(ignite.cluster(),
                maps(Collections.singleton(HOST), SSH_UNAME, pwd, key, 3, U.getIgniteHome(), CFG_NO_ATTR, null),
                null, false, 0, 16);

        assert res.size() == 3;

        F.forEach(res, new CI1<ClusterStartNodeResult>() {
            @Override public void apply(ClusterStartNodeResult t) {
                assert t.getHostName().equals(HOST);

                if (!t.isSuccess())
                    throw new IgniteException(t.getError());
            }
        });

        assert joinedLatch.await(WAIT_TIMEOUT, MILLISECONDS);

        assert ignite.cluster().nodes().size() == 3;

        joinedLatch = new CountDownLatch(3);
        leftLatch = new CountDownLatch(3);

        ignite.cluster().restartNodes();

        assert joinedLatch.await(WAIT_TIMEOUT, MILLISECONDS);
        assert leftLatch.await(WAIT_TIMEOUT, MILLISECONDS);

        assert ignite.cluster().nodes().size() == 3;
    }

    /**
     * @throws Exception If failed.
     */
    public void testRestartNodesFiltered() throws Exception {
        joinedLatch = new CountDownLatch(2);

        Collection<ClusterStartNodeResult> res =
            startNodes(ignite.cluster(),
                maps(Collections.singleton(HOST), SSH_UNAME, pwd, key, 2, U.getIgniteHome(), CFG_ATTR, null),
                null, false, 0, 16);

        assert res.size() == 2;

        F.forEach(res, new CI1<ClusterStartNodeResult>() {
            @Override public void apply(ClusterStartNodeResult t) {
                assert t.getHostName().equals(HOST);

                if (!t.isSuccess())
                    throw new IgniteException(t.getError());
            }
        });

        assert joinedLatch.await(WAIT_TIMEOUT, MILLISECONDS);

        joinedLatch = new CountDownLatch(1);

        res = startNodes(ignite.cluster(),
            maps(Collections.singleton(HOST), SSH_UNAME, pwd, key, 3, U.getIgniteHome(), CFG_NO_ATTR, null),
            null, false, 0, 16);

        assert res.size() == 1;

        F.forEach(res, new CI1<ClusterStartNodeResult>() {
            @Override public void apply(ClusterStartNodeResult t) {
                assert t.getHostName().equals(HOST);

                if (!t.isSuccess())
                    throw new IgniteException(t.getError());
            }
        });

        assert joinedLatch.await(WAIT_TIMEOUT, MILLISECONDS);

        assert ignite.cluster().nodes().size() == 3;

        joinedLatch = new CountDownLatch(2);
        leftLatch = new CountDownLatch(2);

        X.println("Restarting nodes with " + CUSTOM_CFG_ATTR_KEY);

        Collection<UUID> ids = F.transform(ignite.cluster().forAttribute(CUSTOM_CFG_ATTR_KEY, CUSTOM_CFG_ATTR_VAL).nodes(),
            new IgniteClosure<ClusterNode, UUID>() {
                @Override public UUID apply(ClusterNode node) {
                    return node.id();
                }
            }
        );

        ignite.cluster().restartNodes(ids);

        assert leftLatch.await(WAIT_TIMEOUT, MILLISECONDS);
        assert joinedLatch.await(WAIT_TIMEOUT, MILLISECONDS);

        assert ignite.cluster().nodes().size() == 3;
    }

    /**
     * @throws Exception If failed.
     */
    public void testRestartNodeById() throws Exception {
        joinedLatch = new CountDownLatch(3);

        Collection<ClusterStartNodeResult> res =
            startNodes(ignite.cluster(),
                maps(Collections.singleton(HOST), SSH_UNAME, pwd, key, 3, U.getIgniteHome(), CFG_NO_ATTR, null),
                null, false, 0, 16);

        assert res.size() == 3;

        F.forEach(res, new CI1<ClusterStartNodeResult>() {
            @Override public void apply(ClusterStartNodeResult t) {
                assert t.getHostName().equals(HOST);

                if (!t.isSuccess())
                    throw new IgniteException(t.getError());
            }
        });

        assert joinedLatch.await(WAIT_TIMEOUT, MILLISECONDS);

        assert ignite.cluster().nodes().size() == 3;

        joinedLatch = new CountDownLatch(1);
        leftLatch = new CountDownLatch(1);

        ignite.cluster().restartNodes(Collections.singleton(F.first(ignite.cluster().forRemotes().nodes()).id()));

        assert joinedLatch.await(WAIT_TIMEOUT, MILLISECONDS);
        assert leftLatch.await(WAIT_TIMEOUT, MILLISECONDS);

        assert ignite.cluster().nodes().size() == 3;
    }

    /**
     * @throws Exception If failed.
     */
    public void testRestartNodesByIds() throws Exception {
        joinedLatch = new CountDownLatch(3);

        Collection<ClusterStartNodeResult> res =
            startNodes(ignite.cluster(),
                maps(Collections.singleton(HOST), SSH_UNAME, pwd, key, 3, U.getIgniteHome(), CFG_NO_ATTR, null),
                null, false, 0, 16);

        assert res.size() == 3;

        F.forEach(res, new CI1<ClusterStartNodeResult>() {
            @Override public void apply(ClusterStartNodeResult t) {
                assert t.getHostName().equals(HOST);

                if (!t.isSuccess())
                    throw new IgniteException(t.getError());
            }
        });

        assert joinedLatch.await(WAIT_TIMEOUT, MILLISECONDS);

        assert ignite.cluster().nodes().size() == 3;

        joinedLatch = new CountDownLatch(2);
        leftLatch = new CountDownLatch(2);

        Iterator<ClusterNode> it = ignite.cluster().nodes().iterator();

        ignite.cluster().restartNodes(F.asList(it.next().id(), it.next().id()));

        assert joinedLatch.await(WAIT_TIMEOUT, MILLISECONDS);
        assert leftLatch.await(WAIT_TIMEOUT, MILLISECONDS);

        assert ignite.cluster().nodes().size() == 3;
    }

    /**
     * @throws Exception If failed.
     */
    public void testRestartNodesByIdsC() throws Exception {
        joinedLatch = new CountDownLatch(3);

        Collection<ClusterStartNodeResult> res =
            startNodes(ignite.cluster(),
                maps(Collections.singleton(HOST), SSH_UNAME, pwd, key, 3, U.getIgniteHome(), CFG_NO_ATTR, null),
                null, false, 0, 16);

        assert res.size() == 3;

        F.forEach(res, new CI1<ClusterStartNodeResult>() {
            @Override public void apply(ClusterStartNodeResult t) {
                assert t.getHostName().equals(HOST);

                if (!t.isSuccess())
                    throw new IgniteException(t.getError());
            }
        });

        assert joinedLatch.await(WAIT_TIMEOUT, MILLISECONDS);

        assert ignite.cluster().nodes().size() == 3;

        joinedLatch = new CountDownLatch(2);
        leftLatch = new CountDownLatch(2);

        Iterator<ClusterNode> it = ignite.cluster().nodes().iterator();

        ignite.cluster().restartNodes(F.asList(it.next().id(), it.next().id()));

        assert joinedLatch.await(WAIT_TIMEOUT, MILLISECONDS);
        assert leftLatch.await(WAIT_TIMEOUT, MILLISECONDS);

        assert ignite.cluster().nodes().size() == 3;
    }

    /**
     * @param host Hostname.
     * @param uname Username.
     * @param passwd Password.
     * @param key Private key file.
     * @param nodes Number of nodes.
     * @param igniteHome Ignite home.
     * @param cfg Configuration file path.
     * @param script Startup script path.
     * @return Parameters map.
     */
    private Map<String, Object> map(
        String host,
        @Nullable String uname,
        @Nullable String passwd,
        @Nullable File key,
        @Nullable Integer nodes,
        @Nullable String igniteHome,
        @Nullable String cfg,
        @Nullable String script) {
        assert host != null;

        Map<String, Object> params = new HashMap<>();

        params.put(IgniteNodeStartUtils.HOST, host);
        params.put(UNAME, uname);
        params.put(PASSWD, passwd);
        params.put(KEY, key);
        params.put(NODES, nodes);
        params.put(IGNITE_HOME, igniteHome);
        params.put(CFG, cfg);
        params.put(SCRIPT, script);

        return params;
    }

    /**
     * @param hosts Hostnames.
     * @param uname Username.
     * @param passwd Password.
     * @param key Private key file.
     * @param nodes Number of nodes.
     * @param igniteHome Ignite home.
     * @param cfg Configuration file path.
     * @param script Startup script path.
     * @return Parameters map.
     */
    private Collection<Map<String, Object>> maps(
        Collection<String> hosts,
        @Nullable String uname,
        @Nullable String passwd,
        @Nullable File key,
        @Nullable Integer nodes,
        @Nullable String igniteHome,
        @Nullable String cfg,
        @Nullable String script) {
        assert HOST != null;

        Collection<Map<String, Object>> maps = new ArrayList<>(hosts.size());

        for (String host : hosts) {
            Map<String, Object> params = new HashMap<>();

            params.put(IgniteNodeStartUtils.HOST, host);
            params.put(UNAME, uname);
            params.put(PASSWD, passwd);
            params.put(KEY, key);
            params.put(NODES, nodes);
            params.put(IGNITE_HOME, igniteHome);
            params.put(CFG, cfg);
            params.put(SCRIPT, script);

            maps.add(params);
        }

        return maps;
    }

    /**
     * @param name Filename.
     * @return Whether name belongs to log file.
     */
    private boolean isSshNodeLogName(String name) {
        return name.matches("ignite.[0-9a-z-]+.log");
    }

    /**
     * @param cluster Cluster.
     * @param hosts Hosts.
     * @param dflts Default.
     * @param restart Restart flag.
     * @param timeout Timeout.
     * @param maxConn Maximum connections.
     * @return Results collection.
     */
    private Collection<ClusterStartNodeResult> startNodes(IgniteCluster cluster,
        Collection<Map<String, Object>> hosts,
        @Nullable Map<String, Object> dflts,
        boolean restart,
        int timeout,
        int maxConn) {
        cluster = cluster.withAsync();

        assertNull(cluster.startNodes(hosts, dflts, restart, timeout, maxConn));

        return cluster.<Collection<ClusterStartNodeResult>>future().get(WAIT_TIMEOUT);
    }
}