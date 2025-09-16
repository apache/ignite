/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.ignite.internal.processors.query.calcite;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import org.apache.calcite.plan.RelOptListener;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RuleEventLogger;

/** Watches appliying of a rule. */
public class RuleApplyListener extends RuleEventLogger {
    /** */
    private final RelOptRule rule;

    /** */
    public final Collection<RelOptListener.RuleAttemptedEvent> tries = new ArrayList<>();

    /** */
    private final Collection<RelOptRuleCall> products = new HashSet<>();

    /** */
    public RuleApplyListener(RelOptRule rule) {
        this.rule = rule;
    }

    /** {@inheritDoc} */
    @Override public void ruleAttempted(RuleAttemptedEvent evt) {
        assert evt.getRuleCall().getRule() == rule || !rule.getClass().isAssignableFrom(evt.getRuleCall().getRule().getClass());

        if (evt.getRuleCall().getRule() == rule && !evt.isBefore())
            tries.add(evt);

        super.ruleAttempted(evt);
    }

    /** {@inheritDoc} */
    @Override public void ruleProductionSucceeded(RuleProductionEvent evt) {
        assert evt.getRuleCall().getRule() == rule || !rule.getClass().isAssignableFrom(evt.getRuleCall().getRule().getClass());

        if (evt.getRuleCall().getRule() == rule && !evt.isBefore())
            products.add(evt.getRuleCall());

        super.ruleProductionSucceeded(evt);
    }

    /**
     * Checks wheter the rule was sucessfully applied. Resets the listener.
     *
     * @return {@code True} if rule has worked and realeased a product.
     */
    public boolean ruleSucceeded() {
        boolean res = !tries.isEmpty() && tries.stream().allMatch(e -> products.contains(e.getRuleCall()));

        tries.clear();
        products.clear();

        return res;
    }
}
