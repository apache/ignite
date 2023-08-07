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
import java.util.Map;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.hint.Hintable;
import org.apache.calcite.rel.hint.RelHint;
import org.apache.ignite.internal.util.typedef.F;
import org.jetbrains.annotations.Nullable;

/** */
public class HintUtils {
    /** */
    private HintUtils() {
        // No-op.
    }

    /**
     * @return Hint if found by {@code hintDef} in {@code hints}. {@code Null} if hint is not found.
     */
    public static @Nullable RelHint hint(Collection<RelHint> hints, HintDefinition hintDef) {
        if (!F.isEmpty(hints)) {
            for (RelHint h : hints) {
                if (h.hintName.equals(hintDef.name()))
                    return h;
            }
        }

        return null;
    }

    /**
     * @return Hint of {@code rel} if found by {@code hintDef}. {@code Null} if hint is not found.
     */
    public static @Nullable RelHint hint(RelNode rel, HintDefinition hintDef) {
        return rel instanceof Hintable ? hint(((Hintable)rel).getHints(), hintDef) : null;
    }

    /**
     * @return {@code True} if {@code rel} contains hint {@code hintDef}. {@code False} otherwise.
     */
    public static boolean hasHint(RelNode rel, HintDefinition hintDef) {
        return hint(rel, hintDef) != null;
    }

    /**
     * @return {@code Null} if {@code rel} has no hint named as {@code hintDef}. Otherwise, plain options of the hint.
     */
    public static @Nullable Collection<String> plainOptions(RelNode rel, HintDefinition hintDef) {
        RelHint hint = hint(rel, hintDef);

        return hint == null ? null : hint.listOptions;
    }

    /**
     * @return Key-value options of {@code hint}. {@code Null} if {@code hint} is {@code null}. Emply map if hint
     * exists but has no plain options.
     */
    public static Map<String, String> kvOptions(RelHint hint) {
        return hint == null ? null : (hint.kvOptions == null ? Collections.emptyMap() : hint.kvOptions);
    }
}
