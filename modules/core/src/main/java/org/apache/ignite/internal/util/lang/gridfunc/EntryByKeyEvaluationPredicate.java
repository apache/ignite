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

package org.apache.ignite.internal.util.lang.gridfunc;

import java.util.Map;
import org.apache.ignite.internal.util.lang.GridFunc;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.lang.IgnitePredicate;

/**
 * Predicate evaluates to true for given value.
 * Note that evaluation will be short-circuit when first predicate evaluated to false is found.
 */
public class EntryByKeyEvaluationPredicate<K, V> implements IgnitePredicate<Map.Entry<K, V>> {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    private final IgnitePredicate<? super K>[] preds;

    /**
     * @param preds Optional set of predicates to use for filtration. If none provided - original map (or its copy) will be
     * returned.
     */
    public EntryByKeyEvaluationPredicate(IgnitePredicate<? super K>... preds) {
        this.preds = preds;
    }

    /** {@inheritDoc} */
    @Override public boolean apply(Map.Entry<K, V> e) {
        return GridFunc.isAll(e.getKey(), preds);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(EntryByKeyEvaluationPredicate.class, this);
    }
}
