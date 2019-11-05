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

package org.apache.ignite.internal.managers.encryption;

import java.util.UUID;
import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.util.distributed.DistributedProcessActionMessage;
import org.apache.ignite.internal.util.typedef.internal.S;

/**
 * Master key change result message.
 *
 * @see GridEncryptionManager.MasterKeyChangeProcess
 */
public class MasterKeyChangeResult extends MasterKeyChangeAbstractMessage implements DistributedProcessActionMessage {
    /** Error that caused this change to be rejected. */
    private IgniteException err;

    /**
     * @param req Request.
     * @param err Error.
     */
    MasterKeyChangeResult(MasterKeyChangeRequest req, IgniteException err) {
        super(req.requestId(), req.encKeyName(), req.digest());

        this.err = err;
    }

    /** @return Error that caused this change to be rejected. */
    public IgniteException error() {
        return err;
    }

    /** @return {@code True} if has error. */
    public boolean hasError() {
        return err != null;
    }

    /** {@inheritDoc} */
    @Override public UUID requestId() {
        return reqId;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(MasterKeyChangeResult.class, this, "reqId", reqId);
    }
}
