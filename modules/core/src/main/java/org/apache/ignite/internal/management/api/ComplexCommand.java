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

/**
 * Command that have subcommands. Combine {@link Command} and {@link CommandsRegistry} features.
 * Subcommand name must start with the base command name like:
 * <ul>
 *     <li>Base command: {@code StateCommand}.</li>
 *     <li>Subcommand: {@code StateSetCommand}, {@code StateGetCommand}, etc.</li>
 * </ul>
 */
public interface ComplexCommand<A extends IgniteDataTransferObject, R>
    extends Command<A, R>, CommandsRegistry {
    /** {@inheritDoc} */
    @Override public default String description() {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public default Class<A> argClass() {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public default Class taskClass() {
        return null;
    }
}
