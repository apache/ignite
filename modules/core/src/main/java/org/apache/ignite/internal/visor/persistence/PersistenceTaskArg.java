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

package org.apache.ignite.internal.visor.persistence;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

import org.apache.ignite.internal.dto.IgniteDataTransferObject;
import org.apache.ignite.internal.util.typedef.internal.U;

/**
 *
 */
public class PersistenceTaskArg extends IgniteDataTransferObject {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    private PersistenceOperation op;

    /** */
    private PersistenceCleanAndBackupSettings cleanAndBackupSettings;

    /**
     * Default constructor.
     */
    public PersistenceTaskArg() {
        // No-op.
    }

    /**
     * @param op {@link PersistenceOperation} requested for execution.
     * @param cleanAndBackupSettings {@link PersistenceCleanAndBackupSettings} specific settings for clean and backup
     *                                                                        commands.
     */
    public PersistenceTaskArg(PersistenceOperation op, PersistenceCleanAndBackupSettings cleanAndBackupSettings) {
        this.op = op;
        this.cleanAndBackupSettings = cleanAndBackupSettings;
    }

    /**
     * @return {@link PersistenceOperation} operation requested for execution.
     */
    public PersistenceOperation operation() {
        return op;
    }

    /**
     * @return {@link PersistenceCleanAndBackupSettings} specific settings for clean and backup commands.
     */
    public PersistenceCleanAndBackupSettings cleanAndBackupSettings() {
        return cleanAndBackupSettings;
    }

    /** {@inheritDoc} */
    @Override protected void writeExternalData(ObjectOutput out) throws IOException {
        U.writeEnum(out, op);
        out.writeObject(cleanAndBackupSettings);
    }

    /** {@inheritDoc} */
    @Override protected void readExternalData(byte protoVer, ObjectInput in) throws IOException, ClassNotFoundException {
        op = PersistenceOperation.fromOrdinal(in.readByte());
        cleanAndBackupSettings = (PersistenceCleanAndBackupSettings)in.readObject();
    }
}
