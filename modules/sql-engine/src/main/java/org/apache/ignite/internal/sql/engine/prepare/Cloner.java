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

package org.apache.ignite.internal.sql.engine.prepare;

import java.util.ArrayList;
import java.util.List;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.ignite.internal.sql.engine.rel.IgniteReceiver;
import org.apache.ignite.internal.sql.engine.rel.IgniteRel;
import org.apache.ignite.internal.sql.engine.util.Commons;

/**
 * Cloner.
 * TODO Documentation https://issues.apache.org/jira/browse/IGNITE-15859
 */
public class Cloner {
    private final RelOptCluster cluster;

    private List<IgniteReceiver> remotes;

    Cloner(RelOptCluster cluster) {
        this.cluster = cluster;
    }

    /**
     * Clones and associates a plan with a new cluster.
     *
     * @param src Fragment to clone.
     * @return New plan.
     */
    public Fragment go(Fragment src) {
        try {
            remotes = new ArrayList<>();

            IgniteRel newRoot = visit(src.root());

            return new Fragment(src.fragmentId(), newRoot, List.copyOf(remotes), src.serialized(), src.mapping());
        } finally {
            remotes = null;
        }
    }

    /**
     * Clone.
     * TODO Documentation https://issues.apache.org/jira/browse/IGNITE-15859
     */
    public static IgniteRel clone(IgniteRel r) {
        Cloner c = new Cloner(r.getCluster());

        return c.visit(r);
    }

    private IgniteRel collect(IgniteRel rel) {
        if (rel instanceof IgniteReceiver && remotes != null) {
            remotes.add((IgniteReceiver) rel);
        }

        return rel;
    }

    /**
     * Clones and associates a plan with a new cluster.
     *
     * @param rel The head of the relational tree.
     * @return A new tree.
     */
    public IgniteRel visit(IgniteRel rel) {
        return collect(rel.clone(cluster, Commons.transform(rel.getInputs(), rel0 -> visit((IgniteRel) rel0))));
    }
}
