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

package org.apache.ignite.internal.processor.security.compute;

import org.apache.ignite.Ignition;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processor.security.AbstractResolveSecurityContextTest;
import org.apache.ignite.lang.IgniteRunnable;

/**
 * Security tests for an execute server task.
 */
public class ExecuteServiceTaskSecurityTest extends AbstractResolveSecurityContextTest {
    /**
     *
     */
    public void testExecute() {
        assertAllowed(() -> execute(clntAllPerms, clntReadOnlyPerm, "key"));
        assertAllowed(() -> execute(clntAllPerms, srvReadOnlyPerm, "key"));
        assertAllowed(() -> execute(srvAllPerms, clntReadOnlyPerm, "key"));
        assertAllowed(() -> execute(srvAllPerms, srvReadOnlyPerm, "key"));
        assertAllowed(() -> execute(srvAllPerms, srvAllPerms, "key"));
        assertAllowed(() -> execute(clntAllPerms, clntAllPerms, "key"));

        assertForbidden(() -> execute(clntReadOnlyPerm, srvAllPerms, "fail_key"));
        assertForbidden(() -> execute(clntReadOnlyPerm, clntAllPerms, "fail_key"));
        assertForbidden(() -> execute(srvReadOnlyPerm, srvAllPerms, "fail_key"));
        assertForbidden(() -> execute(srvReadOnlyPerm, clntAllPerms, "fail_key"));
        assertForbidden(() -> execute(srvReadOnlyPerm, srvReadOnlyPerm, "fail_key"));
        assertForbidden(() -> execute(clntReadOnlyPerm, clntReadOnlyPerm, "fail_key"));
    }

    /**
     * @param initiator Initiator node.
     * @param remote Remoute node.
     * @param key Key.
     * @return Value that will be to put into cache with passed key.
     */
    private Integer execute(IgniteEx initiator, IgniteEx remote, String key) {
        Integer val = values.getAndIncrement();

        try {
            initiator.executorService(initiator.cluster().forNode(remote.localNode()))
                .submit(
                    new IgniteRunnable() {
                        @Override public void run() {
                            Ignition.localIgnite().cache(CACHE_NAME)
                                .put(key, val);
                        }
                    }
                ).get();
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }

        return val;
    }
}
