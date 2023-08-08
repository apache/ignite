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

package org.apache.ignite.internal.processors.query.calcite.hint;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.apache.calcite.rel.hint.RelHint;
import org.apache.ignite.internal.util.typedef.F;
import org.jetbrains.annotations.Nullable;

/**
 * Collects and holds hints' options.
 */
public final class HintOptions {
    /** */
    public static final HintOptions EMPTY = new HintOptions(-1, Collections.emptySet(), Collections.emptyMap());

    /** Number of hints having no any option. */
    private final int emptyNum;

    /** Plain options. */
    private final Set<String> plain;

    /** Key-value options. */
    private final Map<String, Set<String>> kv;

    /** Ctor. */
    private HintOptions(int emptyNum, Set<String> plain, Map<String, Set<String>> kv) {
        this.emptyNum = emptyNum;
        this.plain = plain;
        this.kv = kv;
    }

    /** */
    public static @Nullable HintOptions collect(Collection<RelHint> hints) {
        if (F.isEmpty(hints))
            return EMPTY;

        int emptyNum = 0;
        Set<String> plainOptions = new HashSet<>();
        Map<String, Set<String>> kvOptions = new HashMap<>();

        for (RelHint h : hints) {
            if (F.isEmpty(h.listOptions) && F.isEmpty(h.kvOptions)) {
                ++emptyNum;

                continue;
            }

            plainOptions.addAll(h.listOptions);

            h.kvOptions.forEach((key, value) -> kvOptions.compute(key, (key0, valSet) -> {
                if (valSet == null)
                    valSet = new HashSet<>();

                valSet.add(value);

                return valSet;
            }));
        }

        return new HintOptions(emptyNum, plainOptions, kvOptions);
    }

    /** */
    public boolean notFound() {
        return this == EMPTY;
    }

    /** */
    public int emptyNum() {
        return emptyNum;
    }

    /** */
    public Set<String> plain() {
        return plain;
    }

    /** */
    public Map<String, Set<String>> kv() {
        return kv;
    }
}
