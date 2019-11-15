/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.query.calcite.serialize;

import org.apache.ignite.internal.processors.query.calcite.rel.IgniteExchange;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteFilter;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteJoin;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteProject;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteRel;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteTableScan;
import org.apache.ignite.internal.processors.query.calcite.rel.Receiver;
import org.apache.ignite.internal.processors.query.calcite.rel.Sender;
import org.apache.ignite.internal.processors.query.calcite.util.Implementor;

/**
 *
 */
public class RelToNodeConverter {
    static class ImplementorImpl implements Implementor<Node> {

        @Override public Node implement(IgniteExchange rel) {
            return null;
        }

        @Override public Node implement(IgniteFilter rel) {
            return null;
        }

        @Override public Node implement(IgniteJoin rel) {
            return null;
        }

        @Override public Node implement(IgniteProject rel) {
            return null;
        }

        @Override public Node implement(IgniteTableScan rel) {
            return null;
        }

        @Override public Node implement(Receiver rel) {
            return null;
        }

        @Override public Node implement(Sender rel) {
            return null;
        }

        @Override public Node implement(IgniteRel other) {
            return null;
        }
    }

    static class ExchangeNode implements Node {
        @Override public IgniteRel toRel(SerializationContext ctx) {
            return null;
        }
    }

    static class FilterNode implements Node {
        @Override public IgniteRel toRel(SerializationContext ctx) {
            return null;
        }
    }

    static class HashJoinNode implements Node {
        @Override public IgniteRel toRel(SerializationContext ctx) {
            return null;
        }
    }

    static class ProjectNode implements Node {
        @Override public IgniteRel toRel(SerializationContext ctx) {
            return null;
        }
    }

    static class TableScanNode implements Node {
        @Override public IgniteRel toRel(SerializationContext ctx) {
            return null;
        }
    }

    static class ReceiverNode implements Node {
        @Override public IgniteRel toRel(SerializationContext ctx) {
            return null;
        }
    }

    static class SenderNode implements Node {
        @Override public IgniteRel toRel(SerializationContext ctx) {
            return null;
        }
    }
}
