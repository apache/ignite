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

package org.apache.ignite.internal.management.persistence;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.function.Consumer;
import org.apache.ignite.internal.dto.IgniteDataTransferObject;
import org.apache.ignite.internal.management.api.CommandRegistryImpl;
import org.apache.ignite.internal.management.api.ComputeCommand;
import org.apache.ignite.internal.management.persistence.PersistenceCommand.PersistenceTaskArg;

/** */
public class PersistenceCommand extends CommandRegistryImpl<PersistenceTaskArg, PersistenceTaskResult>
    implements ComputeCommand<PersistenceTaskArg, PersistenceTaskResult> {
    /** */
    public PersistenceCommand() {
        super(
            new PersistenceInfoCommand(),
            new PersistenceCleanCommand(),
            new PersistenceBackupCommand()
        );
    }

    /** {@inheritDoc} */
    @Override public String description() {
        return "Print information about potentially corrupted caches on local node";
    }

    /** {@inheritDoc} */
    @Override public Class<PersistenceInfoTaskArg> argClass() {
        return PersistenceInfoTaskArg.class;
    }

    /** {@inheritDoc} */
    @Override public Class<PersistenceTask> taskClass() {
        return PersistenceTask.class;
    }

    /** {@inheritDoc} */
    @Override public void printResult(PersistenceTaskArg arg, PersistenceTaskResult res, Consumer<String> printer) {
        new PersistenceInfoCommand().printResult(arg, res, printer);
    }

    /** */
    public abstract static class PersistenceTaskArg extends IgniteDataTransferObject {
        /** */
        private static final long serialVersionUID = 0;

        /** {@inheritDoc} */
        @Override protected void writeExternalData(ObjectOutput out) throws IOException {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override protected void readExternalData(ObjectInput in) throws IOException, ClassNotFoundException {
            // No-op.
        }
    }

    /** */
    public static class PersistenceInfoTaskArg extends PersistenceTaskArg {
        /** */
        private static final long serialVersionUID = 0;
    }

    /** */
    public static class PersistenceCleanCorruptedTaskArg extends PersistenceTaskArg {
        /** */
        private static final long serialVersionUID = 0;
    }

    /** */
    public static class PersistenceCleanAllTaskArg extends PersistenceTaskArg {
        /** */
        private static final long serialVersionUID = 0;
    }

    /** */
    public static class PersistenceBackupCorruptedTaskArg extends PersistenceTaskArg {
        /** */
        private static final long serialVersionUID = 0;
    }

    /** */
    public static class PersistenceBackupAllTaskArg extends PersistenceTaskArg {
        /** */
        private static final long serialVersionUID = 0;
    }
}
