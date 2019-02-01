/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.compatibility.utils;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.lang.IgniteInClosure;
import org.junit.Assert;

/**
 * Command for topology checking.
 */
public class GridCompatibilityCheckTopology implements IgniteInClosure<Ignite> {
    /** */
    private final int expSrvs;

    /** */
    private final int expClients;

    /**
     * @param expSrvs Expected servers number.
     * @param expClients Expected clients number.
     */
    public GridCompatibilityCheckTopology(int expSrvs, int expClients) {
        this.expSrvs = expSrvs;
        this.expClients = expClients;
    }

    /** {@inheritDoc} */
    @Override public void apply(Ignite ignite) {
        IgniteLogger log = ignite.log();

        log.info("Check topology [expSrvs=" + expSrvs + ", expClients=" + expClients + ']');

        Assert.assertEquals("Unexpected server nodes number", expSrvs, ignite.cluster().forServers().nodes().size());
        Assert.assertEquals("Unexpected client nodes number", expClients, ignite.cluster().forClients().nodes().size());
    }
}
