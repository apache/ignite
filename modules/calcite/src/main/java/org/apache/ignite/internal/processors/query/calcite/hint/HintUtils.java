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
import java.util.stream.Collectors;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.hint.Hintable;
import org.apache.calcite.rel.hint.RelHint;
import org.apache.ignite.internal.util.typedef.F;

/** */
public final class HintUtils {
    /** */
    private HintUtils() {
        // No-op.
    }

    /**
     * @return Hints if found by {@code hintDef} in {@code hints}. Empty collection if hints are not found.
     */
    public static Collection<RelHint> hint(Collection<RelHint> hints, HintDefinition hintDef) {
        return hints.stream().filter(h -> h.hintName.equals(hintDef.name())).collect(Collectors.toList());
    }

    /**
     * @return Hints of {@code rel} if found by {@code hintDef}. Empty collection if hints are not found or if
     * {@code rel} is not {@code Hintable}.
     */
    public static Collection<RelHint> hint(RelNode rel, HintDefinition hintDef) {
        return rel instanceof Hintable ? hint(((Hintable)rel).getHints(), hintDef) : Collections.emptyList();
    }

    /**
     * @return {@code True} if {@code rel} contains any hint named as {@code hintDef}. {@code False} otherwise.
     */
    public static boolean hasHint(RelNode rel, HintDefinition hintDef) {
        return !F.isEmpty(hint(rel, hintDef));
    }

    /**
     * @return Combined plain options of all the hints named as {@code hintDef} if any found. Otherwise, an empty
     * collection.
     */
    public static Collection<String> plainOptions(RelNode rel, HintDefinition hintDef) {
        return F.flatCollections(hint(rel, hintDef).stream().map(h -> h.listOptions).collect(Collectors.toList()));
    }
}
