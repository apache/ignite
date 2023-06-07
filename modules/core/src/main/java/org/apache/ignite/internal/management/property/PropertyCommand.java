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

package org.apache.ignite.internal.management.property;

import org.apache.ignite.internal.management.api.CommandRegistryImpl;
import org.apache.ignite.internal.management.api.HelpCommand;

/** Commands to manage distributed properties. */
public class PropertyCommand extends CommandRegistryImpl {
    /** */
    public PropertyCommand() {
        super(
            new PropertyHelpCommand(),
            new PropertyListCommand(),
            new PropertyGetCommand(),
            new PropertySetCommand()
        );
    }

    /** */
    public static class PropertyHelpCommand implements HelpCommand {
        /** {@inheritDoc} */
        @Override public String description() {
            return "Print property command help";
        }
    }
}
