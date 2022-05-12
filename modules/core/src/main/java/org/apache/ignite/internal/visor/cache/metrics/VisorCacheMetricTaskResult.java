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

package org.apache.ignite.internal.visor.cache.metrics;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import org.apache.ignite.internal.dto.IgniteDataTransferObject;

/**
 * Result wrapper for {@link VisorCacheMetricTask}.
 */
public class VisorCacheMetricTaskResult extends IgniteDataTransferObject {
    /** Serial version uid.*/
    private static final long serialVersionUID = 0L;

    /** Task result. */
    private Object result;

    /** Task execution error. */
    private Exception error;

    /**
     * Default constructor.
     */
    public VisorCacheMetricTaskResult() {
        // No-op.
    }

    /**
     * @param result Task execution result.
     */
    public VisorCacheMetricTaskResult(Object result) {
        this.result = result;
    }

    /**
     * @param error Task execution error.
     */
    public VisorCacheMetricTaskResult(Exception error) {
        this.error = error;
    }

    /** {@inheritDoc} */
    @Override protected void writeExternalData(ObjectOutput out) throws IOException {
        out.writeObject(result);
        out.writeObject(error);
    }

    /** {@inheritDoc} */
    @Override protected void readExternalData(byte protoVer, ObjectInput in) throws IOException, ClassNotFoundException {
        result = in.readObject();
        error = (Exception)in.readObject();
    }

    /**
     * Get task result or task execution error.
     */
    public Object result() throws Exception {
        if (error != null)
            throw error;

        return result;
    }
}
