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
package org.hawkore.ignite.lucene.search.condition;

import java.util.Collections;
import java.util.Set;

import org.apache.commons.lang3.StringUtils;
import org.hawkore.ignite.lucene.IndexException;

import com.google.common.base.MoreObjects;

/**
 * The abstract base class for queries directed to a specific field which name should be specified.
 *
 * Known subclasses are: <ul> <li> {@link FuzzyCondition} <li> {@link MatchCondition} <li> {@link PhraseCondition} <li>
 * {@link PrefixCondition} <li> {@link RangeCondition} <li> {@link WildcardCondition} <li> {@link BitemporalCondition}
 * <li> {@link DateRangeCondition} <li> {@link GeoDistanceCondition} <li> {@link GeoBBoxCondition} <li> {@link
 * GeoShapeCondition} </ul>
 *
 * @author Andres de la Pena {@literal <adelapena@stratio.com>}
 */
public abstract class SingleFieldCondition extends Condition {

    /** The name of the field to be matched. */
    public final String field;

    /**
     * Abstract {@link SingleFieldCondition} builder receiving the boost to be used.
     *
     * @param boost the boost for this query clause. Documents matching this clause will (in addition to the normal
     * weightings) have their score multiplied by {@code boost}.
     * @param field the name of the field to be matched
     */
    public SingleFieldCondition(Float boost, String field) {
        super(boost);

        if (StringUtils.isBlank(field)) {
            throw new IndexException("Field name required");
        }

        this.field = field;
    }

    /** {@inheritDoc} */
    public Set<String> postProcessingFields() {
        return Collections.singleton(field);
    }

    /** {@inheritDoc} */
    @Override
    protected MoreObjects.ToStringHelper toStringHelper(Object o) {
        return super.toStringHelper(o).add("field", field);
    }
}
