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

package org.apache.ignite.internal.processors.rest.client.message;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import org.apache.ignite.internal.util.typedef.internal.U;

/**
 * {@code Task} command request.
 */
public class GridClientTaskRequest extends GridClientAbstractMessage {
    /** */
    private static final long serialVersionUID = 0L;

    /** Task name. */
    private String taskName;

    /** Task parameter. */
    private Object arg;

    /** Keep portables flag. */
    private boolean keepPortables;

    /**
     * @return Task name.
     */
    public String taskName() {
        return taskName;
    }

    /**
     * @param taskName Task name.
     */
    public void taskName(String taskName) {
        this.taskName = taskName;
    }

    /**
     * @return Arguments.
     */
    public Object argument() {
        return arg;
    }

    /**
     * @param arg Arguments.
     */
    public void argument(Object arg) {
        this.arg = arg;
    }

    /**
     * @return Keep portables flag.
     */
    public boolean keepPortables() {
        return keepPortables;
    }

    /**
     * @param keepPortables Keep portables flag.
     */
    public void keepPortables(boolean keepPortables) {
        this.keepPortables = keepPortables;
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object o) {
        if (this == o)
            return true;

        if (o == null || getClass() != o.getClass())
            return false;

        GridClientTaskRequest other = (GridClientTaskRequest)o;

        return (taskName == null ? other.taskName == null : taskName.equals(other.taskName)) &&
            arg == null ? other.arg == null : arg.equals(other.arg);
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        return (taskName == null ? 0 : taskName.hashCode()) +
            31 * (arg == null ? 0 : arg.hashCode());
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        super.writeExternal(out);

        U.writeString(out, taskName);

        out.writeObject(arg);
    }

    /** {@inheritDoc} */
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        super.readExternal(in);

        taskName = U.readString(in);

        arg = in.readObject();
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return getClass().getSimpleName() + " [taskName=" + taskName + ", arg=" + arg + "]";
    }
}