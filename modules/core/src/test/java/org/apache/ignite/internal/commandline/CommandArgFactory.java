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

package org.apache.ignite.internal.commandline;

import java.util.Collections;
import org.apache.ignite.internal.commandline.argument.CommandArg;
import org.apache.ignite.internal.commandline.cache.CacheCommand;
import org.apache.ignite.internal.commandline.cache.argument.DistributionCommandArg;
import org.apache.ignite.internal.commandline.cache.argument.IdleVerifyCommandArg;
import org.apache.ignite.internal.commandline.cache.argument.ListCommandArg;
import org.apache.ignite.internal.commandline.cache.argument.ValidateIndexesCommandArg;

/**
 * Command arguments factory used for tests.
 */
public class CommandArgFactory {
    /**
     * @param cmd Cache command.
     * @return Possible arguments for passed cache command {@code cmd} or empty array, if arguments not defined for
     * cache command.
     */
    public static CommandArg[] getArgs(CacheCommand cmd) {
        switch (cmd) {
            case RESET_LOST_PARTITIONS:
            case CONTENTION:
            case HELP:
                return new CommandArg[0];

            case DISTRIBUTION:
                return DistributionCommandArg.values();
            case IDLE_VERIFY:
                return IdleVerifyCommandArg.values();

            case LIST:
                return ListCommandArg.values();

            case VALIDATE_INDEXES:
                return ValidateIndexesCommandArg.values();
        }

        throw new IllegalArgumentException("Unknown cache command " + cmd);
    }

    /** Private constructor */
    private CommandArgFactory() {
        /* No-op. */
    }
}
