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

package org.apache.ignite.internal.processors.cache.checker.objects;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import org.apache.ignite.internal.dto.IgniteDataTransferObject;

/**
 * Container for generic job result contains error message.
 */
public class ExecutionResult<T> extends IgniteDataTransferObject {
    /**
     *
     */
    private static final long serialVersionUID = 0L;

    /** Result. */
    protected T res;

    /** Error message. */
    protected String errorMsg;

    /**
     *
     */
    public ExecutionResult(T res, String errorMsg) {
        this.res = res;
        this.errorMsg = errorMsg;
    }

    /**
     *
     */
    public ExecutionResult() {
    }

    /**
     *
     */
    public ExecutionResult(T res) {
        this.res = res;
    }

    /**
     *
     */
    public ExecutionResult(String errorMsg) {
        this.errorMsg = errorMsg;
    }

    /**
     *
     */
    public String errorMessage() {
        return errorMsg;
    }

    /**
     *
     */
    public T result() {
        return res;
    }

    /** {@inheritDoc} */
    @Override protected void writeExternalData(ObjectOutput out) throws IOException {
        out.writeObject(res);
        out.writeObject(errorMsg);

    }

    /** {@inheritDoc} */
    @Override protected void readExternalData(byte protoVer, ObjectInput in) throws IOException, ClassNotFoundException {
        res = (T)in.readObject();
        errorMsg = (String)in.readObject();
    }
}
