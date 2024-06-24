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

package org.apache.ignite.internal.visor;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import org.apache.ignite.internal.dto.IgniteDataTransferObject;
import org.jetbrains.annotations.Nullable;

/**
 * Management task result.
 */
public class VisorTaskResult<R> extends IgniteDataTransferObject {
    /** Serial version UID. */
    private static final long serialVersionUID = 0L;

    /** Task result. */
    private @Nullable R res;

    /** Error. */
    private @Nullable Exception err;

    /** */
    public VisorTaskResult() {
        // No-op.
    }

    /**
     * @param res Task result.
     * @param err Error.
     */
    public VisorTaskResult(@Nullable R res, @Nullable Exception err) {
        this.res = res;
        this.err = err;
    }

    /**
     * @return Task result.
     * @throws Exception if the task was completed with an error.
     */
    public @Nullable R result() throws Exception {
        if (err != null)
            throw err;

        return res;
    }

    /** {@inheritDoc} */
    @Override protected void writeExternalData(ObjectOutput out) throws IOException {
        out.writeObject(res);
        out.writeObject(err);
    }

    /** {@inheritDoc} */
    @Override protected void readExternalData(byte ver, ObjectInput in) throws IOException, ClassNotFoundException {
        res = (R)in.readObject();
        err = (Exception)in.readObject();
    }
}
