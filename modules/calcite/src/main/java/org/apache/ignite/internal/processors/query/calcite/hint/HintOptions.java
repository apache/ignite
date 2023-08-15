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
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.calcite.rel.hint.RelHint;
import org.jetbrains.annotations.Nullable;

/**
 * Collects and holds hints options.
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
     * @return Combined options set of {@code hints} with natural order. {@code Null} if {@code hints} is empty.
     */
    static @Nullable HintOptions collect(Collection<RelHint> hints, boolean reverse) {
        if (hints.isEmpty())
            return null;

        List<String> plainOptions = new ArrayList<>();
        Map<String, List<String>> kvOptions = new LinkedHashMap<>();

        for (RelHint h : hints) {
            if (h.listOptions.isEmpty() && h.kvOptions.isEmpty())
                continue;

            plainOptions.addAll(h.listOptions);

            h.kvOptions.forEach((key, value) -> kvOptions.compute(key, (key0, valSet) -> {
                if (valSet == null)
                    valSet = new ArrayList<>();

                valSet.add(value);

                return valSet;
            }));
        }

        if(reverse){
            Collections.reverse(plainOptions);

            kvOptions.keySet().forEach(k->{
                kvOptions.compute(k, (key, values)->{
                    Collections.reverse(values);

                    return values;
                });
            });
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
