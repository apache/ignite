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

package org.apache.ignite.internal.processors.query.calcite.util;

import java.util.Collections;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.calcite.rel.hint.Hintable;

/** */
public class HintUtils {
    /** */
    private HintUtils() {
        // No-op.
    }

    /** */
    public static Set<String> disabledRules(Hintable rel) {
        if (rel.getHints().isEmpty())
            return Collections.emptySet();

        return rel.getHints().stream()
            .filter(h -> "DISABLE_RULE".equals(h.hintName))
            .flatMap(h -> h.listOptions.stream())
            .collect(Collectors.toSet());
    }
}
