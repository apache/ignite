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

package org.apache.ignite.internal.suggestions;

import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_PERFORMANCE_SUGGESTIONS_DISABLED;

/**
 * Grid performance suggestions.
 */
public class GridPerformanceSuggestions {
    /** Link to article about Ignite performance tuning */
    private static final String SUGGESTIONS_LINK = "https://apacheignite.readme.io/docs/jvm-and-system-tuning";

    /** */
    private static final boolean disabled = Boolean.getBoolean(IGNITE_PERFORMANCE_SUGGESTIONS_DISABLED);

    /** */
    private final Collection<String> perfs = !disabled ? new LinkedHashSet<String>() : null;

    /** */
    private final Collection<String> suppressed = !disabled ? new HashSet<String>() : null;

    /**
     * @param suggestions Suggestions to add.
     */
    public synchronized void addAll(List<String> suggestions) {
        for (String suggestion : suggestions)
            add(suggestion);
    }

    /**
     * @param sug Suggestion to add.
     */
    public synchronized void add(String sug) {
        add(sug, false);
    }

    /**
     * @param sug Suggestion to add.
     * @param suppress {@code True} to suppress this suggestion.
     */
    public synchronized void add(String sug, boolean suppress) {
        if (disabled)
            return;

        if (!suppress)
            perfs.add(sug);
        else
            suppressed.add(sug);
    }

    /**
     * @param log Log.
     * @param gridName Grid name.
     */
    public synchronized void logSuggestions(IgniteLogger log, @Nullable String gridName) {
        if (disabled)
            return;

        if (!F.isEmpty(perfs) && !suppressed.containsAll(perfs)) {
            U.quietAndInfo(log, "Performance suggestions for grid " +
                (gridName == null ? "" : '\'' + gridName + '\'') + " (fix if possible)");
            U.quietAndInfo(log, "To disable, set -D" + IGNITE_PERFORMANCE_SUGGESTIONS_DISABLED + "=true");

            for (String s : perfs)
                if (!suppressed.contains(s))
                    U.quietAndInfo(log, "  ^-- " + s);

            perfs.clear();
        }

        U.quietAndInfo(log, "Refer to this page for more performance suggestions: " + SUGGESTIONS_LINK);
        U.quietAndInfo(log, "");
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridPerformanceSuggestions.class, this);
    }
}
