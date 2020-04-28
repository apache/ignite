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

package org.apache.ignite.internal.processors.security.sandbox;

import java.security.AccessControlContext;
import java.security.AccessController;
import java.security.AllPermission;
import java.security.Permissions;
import java.security.PrivilegedAction;
import java.security.PrivilegedExceptionAction;
import java.util.Collection;
import java.util.Collections;
import java.util.UUID;
import java.util.function.Supplier;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteException;
import org.apache.ignite.Ignition;
import org.apache.ignite.internal.processors.security.impl.TestSecurityPluginProvider;
import org.apache.ignite.internal.util.typedef.G;
import org.junit.Test;

import static org.apache.ignite.plugin.security.SecurityPermissionSetBuilder.ALLOW_ALL;

/**
 * If the Ignite Sandbox is enabled, then {@link Ignition} methods that return existing instances of Ignite should
 * return a component proxy.
 */
public class IgnitionComponentProxyTest extends AbstractSandboxTest {
    /** Server node name. */
    private static final String SRV = "sandbox.IgnitionComponentProxyTest";

    /** Client node name. */
    private static final String CLNT = "clnt";

    /** {@inheritDoc} */
    @Override protected void prepareCluster() throws Exception {
        startGrid(SRV, ALLOW_ALL, false);
        startGrid(CLNT, ALLOW_ALL, true);
    }

    /**
     *
     */
    @Test
    public void testLocalIgnite() {
        Supplier<Ignite> s = new Supplier<Ignite>() {
            @Override public Ignite get() {
                return Ignition.localIgnite();
            }
        };

        check(s);
    }

    /**
     *
     */
    @Test
    public void testGetOrStart() {
        Supplier<Ignite> s = new Supplier<Ignite>() {
            @Override public Ignite get() {
                try {
                    return Ignition.getOrStart(Ignition.localIgnite().configuration());
                }
                catch (Exception e) {
                    throw new IgniteException(e);
                }
            }
        };

        check(s);
    }

    /**
     *
     */
    @Test
    public void testIgniteWithName() {
        Supplier<Ignite> s = new Supplier<Ignite>() {
            @Override public Ignite get() {
                return Ignition.ignite(SRV);
            }
        };

        check(s);
    }

    /**
     *
     */
    @Test
    public void testIgniteWithID() {
        final UUID srvId = grid(SRV).localNode().id();

        Supplier<Ignite> s = new Supplier<Ignite>() {
            @Override public Ignite get() {
                return Ignition.ignite(srvId);
            }
        };

        check(s);
    }

    /**
     *
     */
    @Test
    public void testAllGrids() {
        Supplier<Collection<Ignite>> s = new Supplier<Collection<Ignite>>() {
            @Override public Collection<Ignite> get() {
                return Ignition.allGrids();
            }
        };

        checkCollection(s);
    }

    /**
     *
     */
    @Test
    public void testStart() {
        Supplier<Ignite> s = new Supplier<Ignite>() {
            @Override public Ignite get() {
                try {
                    Permissions perms = new Permissions();

                    perms.add(new AllPermission());

                    AccessControlContext acc = AccessController.doPrivileged(
                        (PrivilegedAction<AccessControlContext>)() -> new AccessControlContext(AccessController.getContext(),
                            new IgniteDomainCombiner(perms)));

                    return AccessController.doPrivileged((PrivilegedExceptionAction<Ignite>)
                        () -> {
                            String login = "node_" + G.allGrids().size();

                            return Ignition.start(
                                optimize(getConfiguration(login,
                                    new TestSecurityPluginProvider(login, "", ALLOW_ALL, perms, globalAuth)))
                            );
                        }, acc);
                }
                catch (Exception e) {
                    throw new IgniteException(e);
                }
            }
        };

        check(s);
    }

    /**
     * @param s Supplier that returns an instance of Ignite.
     */
    private void check(final Supplier<Ignite> s) {
        Supplier<Collection<Ignite>> supplier = new Supplier<Collection<Ignite>>() {
            @Override public Collection<Ignite> get() {
                return Collections.singleton(s.get());
            }
        };

        checkCollection(supplier);
    }

    /**
     * @param s Supplier that returns a collection of Ignite instances.
     */
    private void checkCollection(Supplier<Collection<Ignite>> s) {
        Ignite srv = grid(SRV);

        Ignite clnt = grid(CLNT);

        // Checks that inside the sandbox we should get a proxied instance of Ignite.
        clnt.compute(clnt.cluster().forNodeId(srv.cluster().localNode().id()))
            .broadcast(() -> {
                Collection<Ignite> nodes = s.get();

                for (Ignite node : nodes) {
                    // AccessControlException will be thrown if a non-proxy instance of Ignite runs compute.
                    node.compute().call(() -> true);
                }
            });
    }
}
