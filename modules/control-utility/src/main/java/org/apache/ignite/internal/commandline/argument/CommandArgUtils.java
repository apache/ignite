/*
 *  Licensed to the Apache Software Foundation (ASF) under one or more
 *  contributor license agreements. See the NOTICE file distributed with
 *  this work for additional information regarding copyright ownership.
 *  The ASF licenses this file to You under the Apache License, Version 2.0
 *  (the "License"); you may not use this file except in compliance with
 *  the License. You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.ignite.internal.commandline.argument;

import org.jetbrains.annotations.Nullable;

/**
 * Utility class for control.sh arguments.
 */
public class CommandArgUtils {
    /**
     * Tries convert {@code text} to one of values {@code enumClass}.
     * @param text Input test.
     * @param enumClass {@link CommandArg} enum class.
     * @param <E>
     * @return Converted argument or {@code null} if convert failed.
     */
    public static <E extends Enum<E> & CommandArg> @Nullable E of(String text, Class<E> enumClass) {
        for (E e : enumClass.getEnumConstants()) {
            if (e.argName().equalsIgnoreCase(text))
                return e;
        }

        return null;
    }

    /** Private constructor. */
    private CommandArgUtils() {
        /* No-op. */
    }
}
