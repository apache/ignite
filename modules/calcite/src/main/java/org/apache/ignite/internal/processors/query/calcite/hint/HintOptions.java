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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.calcite.rel.hint.RelHint;
import org.apache.ignite.internal.util.typedef.F;
import org.jetbrains.annotations.Nullable;

/**
 * Collects and holds hints options. Recognizes dot-sepatated entries of quoted {@code RelHint.listOptions} and values
 * of {@code RelHint.kvOptions}.
 */
public final class HintOptions {
    /** Plain options. */
    private final List<String> plain;

    /** Key-value options. */
    private final Map<String, List<String>> kv;

    /** Ctor. */
    private HintOptions(List<String> plain, Map<String, List<String>> kv) {
        this.plain = Collections.unmodifiableList(plain);
        this.kv = Collections.unmodifiableMap(kv);
    }

    /**
     * @return Combined options of {@code hints} with natural order. {@code Null} if {@code hints} is empty.
     */
    static @Nullable HintOptions collect(Collection<RelHint> hints) {
        if (hints.isEmpty())
            return null;

        List<String> plainOptions = new ArrayList<>();
        Map<String, List<String>> kvOptions = new LinkedHashMap<>();

        for (RelHint h : hints) {
            if (h.listOptions.isEmpty() && h.kvOptions.isEmpty())
                continue;

            // Splits plain options by dot.
            plainOptions.addAll(F.flatCollections(h.listOptions.stream().map(opt -> Arrays.stream(opt.split(","))
                .collect(Collectors.toList())).collect(Collectors.toList())));

            h.kvOptions.forEach((key, value) -> kvOptions.compute(key, (key0, valSet) -> {
                if (valSet == null)
                    valSet = new ArrayList<>();

                // Splits quoted key-value options by dot.
                valSet.addAll(Arrays.stream(value.split(",")).filter(opt -> !opt.trim().isEmpty())
                    .collect(Collectors.toList()));

                return valSet;
            }));
        }

        return new HintOptions(plainOptions, kvOptions);
    }

    /** */
    public boolean empty() {
        return plain.isEmpty() && kv.isEmpty();
    }

    /** */
    public List<String> plain() {
        return plain;
    }

    /** */
    public Map<String, List<String>> kv() {
        return kv;
    }
}
