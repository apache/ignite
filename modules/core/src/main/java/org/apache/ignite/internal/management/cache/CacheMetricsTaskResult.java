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

package org.apache.ignite.internal.management.cache;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Collections;
import java.util.Map;
import org.apache.ignite.internal.dto.IgniteDataTransferObject;
import org.apache.ignite.internal.util.typedef.internal.U;

/**
 * Result wrapper for {@link CacheMetricsTask}.
 */
public class CacheMetricsTaskResult extends IgniteDataTransferObject {
    /** Serial version uid. */
    private static final long serialVersionUID = 0L;

    /** Task result. */
    private Map<String, Boolean> result;

    /** Task execution error. */
    private Exception error;

    /**
     * Default constructor.
     */
    public CacheMetricsTaskResult() {
        // No-op.
    }

    /**
     * @param result Task execution result.
     */
    public CacheMetricsTaskResult(Map<String, Boolean> result) {
        this.result = Collections.unmodifiableMap(result);
    }

    /**
     * @param error Task execution error.
     */
    public CacheMetricsTaskResult(Exception error) {
        this.error = error;
    }

    /** {@inheritDoc} */
    @Override protected void writeExternalData(ObjectOutput out) throws IOException {
        U.writeMap(out, result);
        out.writeObject(error);
    }

    /** {@inheritDoc} */
    @Override protected void readExternalData(byte protoVer, ObjectInput in) throws IOException, ClassNotFoundException {
        result = U.readMap(in);
        error = (Exception)in.readObject();
    }

    /**
     * Get task result or task execution error.
     */
    public Map<String, Boolean> result() throws Exception {
        if (error != null)
            throw error;

        return Collections.unmodifiableMap(result);
    }
}
