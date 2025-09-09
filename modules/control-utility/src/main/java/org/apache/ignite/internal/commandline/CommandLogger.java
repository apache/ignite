/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.commandline;

import java.util.stream.Collectors;
import org.apache.ignite.compute.ComputeUserUndeclaredException;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.internal.util.typedef.internal.U;

/**
 * Utility class for creating {@code CommangHandler} log messages.
 */
public class CommandLogger {
    /**
     * Generates readable error message from exception.
     * @param ex Exception.
     * @return Error message.
     */
    public static String errorMessage(Throwable ex) {
        return X.getThrowableList(ex).stream()
            .filter(e -> !(e instanceof ComputeUserUndeclaredException))
            .map(e -> F.isEmpty(e.getMessage()) ? e.getClass().getName() : e.getMessage())
            .distinct()
            .collect(Collectors.joining(U.nl()));
    }
}
