/*
 * Copyright (C) 2014 Stratio (http://stratio.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.hawkore.ignite.lucene.builder.search.condition;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * A {@link Condition} that matches documents matching boolean combinations of other queries, e.g. {@link
 * MatchCondition}s, {@link RangeCondition}s or other {@link BooleanCondition}s.
 *
 * @author Andres de la Pena {@literal <adelapena@stratio.com>}
 */
public class BooleanCondition extends Condition<BooleanCondition> {

    /** The mandatory conditions. */
    @JsonProperty("must")
    private List<Condition> must;

    /** The optional conditions. */
    @JsonProperty("should")
    private List<Condition> should;

    /** The mandatory not conditions. */
    @JsonProperty("not")
    private List<Condition> not;

    /**
     * Returns this with the specified mandatory conditions.
     *
     * @param conditions the mandatory conditions to be added
     * @return this with the specified mandatory conditions
     */
    public BooleanCondition must(Condition... conditions) {
        must = add(must, conditions);
        return this;
    }

    /**
     * Returns this with the specified optional conditions.
     *
     * @param conditions the optional conditions to be added
     * @return this with the specified optional conditions
     */
    public BooleanCondition should(Condition... conditions) {
        should = add(should, conditions);
        return this;
    }

    /**
     * Returns this with the specified mandatory not conditions.
     *
     * @param conditions the mandatory not conditions to be added
     * @return this with the specified mandatory not conditions
     */
    public BooleanCondition not(Condition... conditions) {
        not = add(not, conditions);
        return this;
    }
}
