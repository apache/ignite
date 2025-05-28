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

package org.apache.ignite.util;

import java.util.Collection;
import java.util.Collections;
import org.apache.ignite.internal.management.api.Command;
import org.apache.ignite.internal.management.api.CommandsProvider;
import org.apache.ignite.internal.util.typedef.F;

import static org.apache.ignite.util.GridCommandHandlerTest.ENABLE_TEST_COMMANDS;

/**
 * Enables registration of additional commands for testing purposes. Commands are only registered when
 * the {@code ENABLE_TEST_COMMANDS} system property is set to {@code true}.
 */
public class TestCommandsProvider implements CommandsProvider {
    /** {@inheritDoc} */
    @Override public Collection<Command<?, ?>> commands() {
        if (!Boolean.getBoolean(ENABLE_TEST_COMMANDS))
            return Collections.emptyList();

        return F.asList(
            new GridCommandHandlerTest.OfflineTestCommand()
        );
    }
}
