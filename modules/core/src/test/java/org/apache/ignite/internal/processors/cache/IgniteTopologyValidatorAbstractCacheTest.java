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

package org.apache.ignite.internal.processors.cache;

import org.apache.ignite.*;
import org.apache.ignite.cluster.*;
import org.apache.ignite.configuration.*;
import org.apache.ignite.transactions.*;

import javax.cache.*;
import java.io.*;
import java.util.*;

/**
 * Topology validator test
 */
public abstract class IgniteTopologyValidatorAbstractCacheTest extends IgniteCacheAbstractTest implements Serializable {
    /** key-value used at test */
    protected static String KEY_VALUE = "1";

    /** {@inheritDoc} */
    @Override protected int gridCount() {
        return 1;
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration iCfg = super.getConfiguration(gridName);

        for (CacheConfiguration cCfg : iCfg.getCacheConfiguration()) {
            cCfg.setTopologyValidator(new TopologyValidator() {
                @Override public boolean validate(Collection<ClusterNode> nodes) {
                    return nodes.size() >= 2;
                }
            });
        }

        return iCfg;
    }

    /**
     * Puts before topology is valid.
     */
    protected void putBefore(Transaction tx) {
        try {
            jcache().put(KEY_VALUE, KEY_VALUE);

            if (tx != null)
                tx.commit();

            assert false : "topology validation broken";
        }
        catch (IgniteException | CacheException ex) {
            assert ex.getCause() instanceof IgniteCheckedException && ex.getCause().getMessage().contains("cache topology is not valid");
        }
    }

    /**
     * Puts when topology is valid.
     */
    protected void putAfter(Transaction tx) {
        try {
            assert jcache().get(KEY_VALUE) == null;

            jcache().put(KEY_VALUE, KEY_VALUE);

            if (tx != null)
                tx.commit();

            assert jcache().get(KEY_VALUE).equals(KEY_VALUE);
        }
        catch (IgniteException | CacheException ex) {
            assert false : "topology validation broken";
        }
    }

    /** topology validator test */
    public void testTopologyValidator() throws Exception {

        putBefore(null);

        startGrid(1);

        putAfter(null);
    }
}
