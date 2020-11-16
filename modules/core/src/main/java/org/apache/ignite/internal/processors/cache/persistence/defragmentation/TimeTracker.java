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

package org.apache.ignite.internal.processors.cache.persistence.defragmentation;

import java.util.Arrays;
import org.apache.ignite.internal.util.typedef.internal.U;

/** */
public class TimeTracker<Stage extends Enum<Stage>> {
    /** */
    private final Class<Stage> stageCls;

    /** */
    private final long[] t;

    /** */
    private long lastStage;

    /** */
    public TimeTracker(Class<Stage> stageCls) {
        this.stageCls = stageCls;

        t = new long[stageCls.getEnumConstants().length];

        lastStage = System.nanoTime();
    }

    /** */
    public void complete(Stage stage) {
        int ordinal = stage.ordinal();

        if (ordinal == 0) {
            Arrays.fill(t, 0L);

            lastStage = System.nanoTime();
        }
        else {
            long now = System.nanoTime();

            t[ordinal] += (now - lastStage);

            lastStage = now;
        }
    }

    /** */
    public long getMillis(Stage stage) {
        return U.nanosToMillis(t[stage.ordinal()]);
    }

    /** {@inheritDoc} */
    // TODO Remove. This version sucks.
    @Override public String toString() {
        StringBuilder sb = new StringBuilder("|> {");

        for (Stage s : stageCls.getEnumConstants()) {
            sb.append(s.name().toLowerCase()).append("=")
                .append(U.nanosToMillis(t[s.ordinal()]) * 1e-3).append("s. ");
        }

        sb.append("}");

        return sb.toString();
    }
}
