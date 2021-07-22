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

package org.apache.ignite.internal.processors.query.calcite.prepare;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import org.apache.ignite.internal.processors.query.calcite.metadata.ColocationGroup;
import org.apache.ignite.internal.processors.query.calcite.metadata.FragmentMapping;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteReceiver;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteSender;

import static org.apache.ignite.internal.util.CollectionUtils.nullOrEmpty;
import static org.apache.ignite.internal.util.IgniteUtils.newHashMap;

/**
 *
 */
public abstract class AbstractMultiStepPlan implements MultiStepPlan {
    /** */
    protected final FieldsMetadata fieldsMetadata;

    /** */
    protected final QueryTemplate queryTemplate;

    /** */
    protected ExecutionPlan executionPlan;

    /** */
    protected AbstractMultiStepPlan(QueryTemplate queryTemplate, FieldsMetadata fieldsMetadata) {
        this.queryTemplate = queryTemplate;
        this.fieldsMetadata = fieldsMetadata;
    }

    /** {@inheritDoc} */
    @Override public List<Fragment> fragments() {
        return Objects.requireNonNull(executionPlan).fragments();
    }

    /** {@inheritDoc} */
    @Override public FieldsMetadata fieldsMetadata() {
        return fieldsMetadata;
    }

    /** {@inheritDoc} */
    @Override public FragmentMapping mapping(Fragment fragment) {
        return mapping(fragment.fragmentId());
    }

    /** {@inheritDoc} */
    @Override public ColocationGroup target(Fragment fragment) {
        if (fragment.rootFragment())
            return null;

        IgniteSender sender = (IgniteSender)fragment.root();
        return mapping(sender.targetFragmentId()).findGroup(sender.exchangeId());
    }

    /** {@inheritDoc} */
    @Override public Map<Long, List<String>> remotes(Fragment fragment) {
        List<IgniteReceiver> remotes = fragment.remotes();

        if (nullOrEmpty(remotes))
            return null;

        HashMap<Long, List<String>> res = newHashMap(remotes.size());

        for (IgniteReceiver remote : remotes)
            res.put(remote.exchangeId(), mapping(remote.sourceFragmentId()).nodeIds());

        return res;
    }

    /** {@inheritDoc} */
    @Override public void init(PlanningContext ctx) {
        executionPlan = queryTemplate.map(ctx);
    }

    /** */
    private FragmentMapping mapping(long fragmentId) {
        return Objects.requireNonNull(executionPlan).fragments().stream()
            .filter(f -> f.fragmentId() == fragmentId)
            .findAny().orElseThrow(() -> new IllegalStateException("Cannot find fragment with given ID. [" +
                "fragmentId=" + fragmentId + ", " +
                "fragments=" + fragments() + "]"))
            .mapping();
    }
}
