/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.ignite.raft.jraft.error;

import org.apache.ignite.raft.jraft.Status;
import org.apache.ignite.raft.jraft.entity.EnumOutter;

/**
 * Internal RAFT exception.
 */
public class RaftException extends Throwable {
    private static final long serialVersionUID = -1533343555230409704L;

    /**
     * Error type
     */
    private EnumOutter.ErrorType type;

    /**
     * Error status
     */
    private Status status = Status.OK();

    public RaftException(EnumOutter.ErrorType type) {
        super(type.name());
        this.type = type;
    }

    public RaftException(EnumOutter.ErrorType type, Status status) {
        super(status != null ? status.getErrorMsg() : type.name());
        this.type = type;
        this.status = status;
    }

    public RaftException(EnumOutter.ErrorType type, RaftError err, String fmt, Object... args) {
        super(String.format(fmt, args));
        this.type = type;
        this.status = new Status(err, fmt, args);
    }

    public RaftException() {
        this.type = EnumOutter.ErrorType.ERROR_TYPE_NONE;
        this.status = Status.OK();
    }

    public EnumOutter.ErrorType getType() {
        return this.type;
    }

    public void setType(EnumOutter.ErrorType type) {
        this.type = type;
    }

    public Status getStatus() {
        return this.status;
    }

    public void setStatus(Status status) {
        this.status = status;
    }

    @Override
    public String toString() {
        return "Error [type=" + this.type + ", status=" + this.status + "]";
    }
}
