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

package org.apache.ignite.internal.management.api;

import org.apache.ignite.internal.dto.IgniteDataTransferObject;
import org.jetbrains.annotations.Nullable;

/**
 * Management command interface.<p>
 * Implementations represent single action to manage Ignite cluster.<p>
 *
 * Name of the command that is expected from caller derived from actual command class name.<br>
 * <ul>
 *     <li><b>Name format:</b> All words divided by capital letters except "Command" suffix will form hierarchical command name.</li>
 *     <li><b>Example:</b> {@code MyUsefullCommand} is name of command so {@code control.sh --my-usefull param1 param2}
 *     expected from user.</li>
 * </ul>
 *
 * Other protocols must expose command similarly. Rest API must expect {@code /api-root/my-usefull?param1=value1&param2=value2} URI.
 *
 * @param <A> Argument type.
 * @param <R> Result type.
 */
public interface Command<A extends IgniteDataTransferObject, R> {
    /** */
    public static String CMD_NAME_POSTFIX = "Command";

    /** Command description. */
    public String description();

    /** @return Arguments class. */
    public Class<? extends A> argClass();

    /**
     * @param arg Command argument.
     * @return Message text to show user for. If {@code null} it means that confirmation is not needed.
     */
    public default @Nullable String confirmationPrompt(A arg) {
        return null;
    }

    /**
     * @param arg Command argument.
     * @return Deprecation message if command on the way to being decomissioned.
     */
    public default @Nullable String deprecationMessage(A arg) {
        return null;
    }
}
