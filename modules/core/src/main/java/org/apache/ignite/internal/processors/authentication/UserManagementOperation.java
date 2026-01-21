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

package org.apache.ignite.internal.processors.authentication;

import java.io.Serializable;
import java.util.Objects;
import org.apache.ignite.internal.Order;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.plugin.extensions.communication.Message;

/**
 * The operation with users. Used to deliver the information about requested operation to all server nodes.
 */
public class UserManagementOperation implements Serializable, Message {
    /** */
    private static final long serialVersionUID = 0L;

    /** User. */
    @Order(value = 0, method = "user")
    private User usr;

    /** Operation type. */
    @Order(1)
    private OperationType type;

    /** Operation ID. */
    @Order(2)
    private IgniteUuid id;

    /**
     * Constructor.
     */
    public UserManagementOperation() {
        // No-op.
    }

    /**
     * @param usr User.
     * @param type Action type.
     */
    public UserManagementOperation(User usr, OperationType type) {
        this.usr = usr;
        this.type = type;
        id = IgniteUuid.randomUuid();
    }

    /**
     * @return User.
     */
    public User user() {
        return usr;
    }

    /**
     * @param usr User.
     */
    public void user(User usr) {
        this.usr = usr;
    }

    /**
     * @return Operation type.
     */
    public OperationType type() {
        return type;
    }

    /**
     * @param type Operation type.
     */
    public void type(OperationType type) {
        this.type = type;
    }

    /**
     * @return Operation ID.
     */
    public IgniteUuid id() {
        return id;
    }

    /**
     * @param id Operation ID.
     */
    public void id(IgniteUuid id) {
        this.id = id;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(UserManagementOperation.class, this);
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object o) {
        if (this == o)
            return true;

        if (o == null || getClass() != o.getClass())
            return false;

        UserManagementOperation op = (UserManagementOperation)o;

        return Objects.equals(id, op.id);
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        return id.hashCode();
    }

    /** {@inheritDoc} */
    @Override public short directType() {
        return -103;
    }

    /**
     * User action type.
     */
    public enum OperationType { ADD, UPDATE, REMOVE }
}
