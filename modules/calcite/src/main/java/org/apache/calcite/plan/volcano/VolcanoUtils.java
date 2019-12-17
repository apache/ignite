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

package org.apache.calcite.plan.volcano;

import org.apache.calcite.plan.RelTraitSet;

/**
 * Utility methods to exploit package private API.
 */
public class VolcanoUtils {
    /**
     * Requests an alternative subset with relational nodes, satisfying required traits.
     * @param subset Original subset.
     * @param traits Required traits.
     * @return Result subset, what contains relational nodes, satisfying required traits.
     */
    public static RelSubset subset(RelSubset subset, RelTraitSet traits) {
        return subset.set.getOrCreateSubset(subset.getCluster(), traits.simplify());
    }

    /**
     * Utility method, sets {@link VolcanoPlanner#impatient} parameter to {@code true}.
     * @param planner Planer.
     * @return Planer for chaining.
     */
    public static VolcanoPlanner impatient(VolcanoPlanner planner) {
        planner.impatient = true;

        return planner;
    }
}
