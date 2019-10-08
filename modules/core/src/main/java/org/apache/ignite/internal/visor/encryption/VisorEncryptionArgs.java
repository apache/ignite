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

package org.apache.ignite.internal.visor.encryption;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import org.apache.ignite.internal.commandline.encryption.EncryptionSubcommand;
import org.apache.ignite.internal.dto.IgniteDataTransferObject;
import org.apache.ignite.internal.util.typedef.internal.U;

/**
 * Arguments for task {@link VisorEncryptionTask}
 */
public class VisorEncryptionArgs extends IgniteDataTransferObject {
    /** */
    private static final long serialVersionUID = 0L;

    /** Encryption subcommand. */
    private EncryptionSubcommand cmd;

    /** Master key id. */
    private String masterKeyId;

    /** */
    public VisorEncryptionArgs() {
        // No-op.
    }

    /**
     * @param cmd Encryption subcommand.
     */
    public VisorEncryptionArgs(EncryptionSubcommand cmd) {
        this.cmd = cmd;
    }

    /**
     * @param cmd Encryption subcommand.
     * @param masterKeyId Master key id.
     */
    public VisorEncryptionArgs(EncryptionSubcommand cmd, String masterKeyId) {
        this.cmd = cmd;
        this.masterKeyId = masterKeyId;
    }

    /** @return Encryption subcommand. */
    public EncryptionSubcommand getCmd() {
        return cmd;
    }

    /** @return Master key id. */
    public String getMasterKeyId() {
        return masterKeyId;
    }

    /** {@inheritDoc} */
    @Override protected void writeExternalData(ObjectOutput out) throws IOException {
        out.writeObject(cmd);

        U.writeString(out, masterKeyId);
    }

    /** {@inheritDoc} */
    @Override protected void readExternalData(byte protoVer, ObjectInput in)
        throws IOException, ClassNotFoundException {
        cmd = (EncryptionSubcommand)in.readObject();

        masterKeyId = U.readString(in);
    }
}
