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

package org.apache.ignite.internal.commandline.snapshot;

import java.util.Map;
import java.util.logging.Logger;
import org.apache.ignite.internal.client.GridClient;
import org.apache.ignite.internal.client.GridClientConfiguration;
import org.apache.ignite.internal.commandline.AbstractCommand;
import org.apache.ignite.internal.commandline.Command;
import org.apache.ignite.internal.commandline.CommandArgIterator;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.visor.snapshot.VisorSnapshotTaskResult;

import static org.apache.ignite.internal.commandline.TaskExecutor.executeTaskByNameOnNode;

/**
 * Snapshot sub-command base.
 */
public abstract class SnapshotSubcommand extends AbstractCommand<Object> {
    /** Snapshot name argument. */
    protected static final String SNAPSHOT_NAME_ARG = "snapshot_name";

    /** Command argument. */
    protected Object cmdArg;

    /** Sub-command name. */
    private final String name;

    /** Snapshot visor task class. */
    private final Class<?> taskCls;

    /**
     * @param name Sub-command name.
     * @param taskCls Visor compute task class.
     */
    protected SnapshotSubcommand(String name, Class<?> taskCls) {
        this.name = name;
        this.taskCls = taskCls;
    }

    /** {@inheritDoc} */
    @Override public Object execute(GridClientConfiguration clientCfg, Logger log) throws Exception {
        try (GridClient client = Command.startClient(clientCfg)) {
            VisorSnapshotTaskResult taskRes = executeTaskByNameOnNode(client, taskCls.getName(), arg(), null, clientCfg);

            printResult(taskRes.result(), log);

            return taskRes.result();
        }
    }

    /** {@inheritDoc} */
    @Override public Object arg() {
        return cmdArg;
    }

    /** {@inheritDoc} */
    @Override public void parseArguments(CommandArgIterator argIter) {
        cmdArg = argIter.nextArg("Expected snapshot name.");

        if (argIter.hasNextSubArg())
            throw new IllegalArgumentException("Unexpected argument: " + argIter.peekNextArg() + '.');
    }

    /** {@inheritDoc} */
    @Override public String name() {
        return name;
    }

    /**
     * @return General usage options.
     */
    protected Map<String, String> generalUsageOptions() {
        return F.asMap(
            SNAPSHOT_NAME_ARG,
            "Snapshot name. In case incremental snapshot (--incremental) base snapshot name must be provided"
        );
    }

    /**
     * Prints result of command execution.
     *
     * @param res Task result.
     * @param log Logger.
     */
    protected void printResult(Object res, Logger log) {
        log.info(String.valueOf(res));
    }
}
