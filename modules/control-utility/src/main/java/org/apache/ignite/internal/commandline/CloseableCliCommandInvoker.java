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

import java.util.function.Consumer;
import org.apache.ignite.internal.management.api.CommandInvoker;

/**
 * CLI command invoker.
 */
public interface CloseableCliCommandInvoker extends AutoCloseable {
    /** @return Message text to show user for. {@code null} means that confirmantion is not required. */
    String confirmationPrompt();

    /** @see CommandInvoker#prepare(Consumer) */
    boolean prepare(Consumer<String> printer) throws Exception;

    /** @see #invoke(Consumer) */
    <R> R invoke(Consumer<String> printer) throws Exception;

    /** */
    <R> R invokeBeforeNodeStart(Consumer<String> printer) throws Exception;
}
