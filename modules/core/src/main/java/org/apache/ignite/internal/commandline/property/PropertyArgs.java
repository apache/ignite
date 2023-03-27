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

package org.apache.ignite.internal.commandline.property;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import org.apache.ignite.internal.dto.IgniteDataTransferObject;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.jetbrains.annotations.Nullable;

/** */
public class PropertyArgs extends IgniteDataTransferObject {
    /** Serial version uid. */
    private static final long serialVersionUID = 0L;

    /** Property name argument name. */
    public static final String NAME = "--name";

    /** Type value argument name . */
    public static final String VAL = "--val";

    /** Property name . */
    private String name;

    /** Property's value. */
    private String val;

    /** Action. */
    private Action action;

    /**
     * Default constructor.
     */
    public PropertyArgs() {
        // No-op.
    }

    /** */
    public PropertyArgs(String name, String val, Action action) {
        assert name != null;

        this.name = name;
        this.val = val;
        this.action = action;
    }

    /** */
    public String name() {
        return name;
    }

    /** */
    public String value() {
        return val;
    }

    /** */
    public Action action() {
        return action;
    }

    /** {@inheritDoc} */
    @Override protected void writeExternalData(ObjectOutput out) throws IOException {
        U.writeString(out, name);
        U.writeString(out, val);
        out.writeByte(action.ordinal());
    }

    /** {@inheritDoc} */
    @Override protected void readExternalData(byte protoVer, ObjectInput in) throws IOException, ClassNotFoundException {
        name = U.readString(in);
        val = U.readString(in);
        action = Action.fromOrdinal(in.readByte());
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return name + "=" + val;
    }

    /**
     *
     */
    public enum Action {
        /** */
        GET,

        /** */
        SET;

        /** Enumerated values. */
        private static final Action[] VALS = values();

        /**
         * Efficiently gets enumerated value from its ordinal.
         */
        @Nullable public static Action fromOrdinal(int ord) {
            return ord >= 0 && ord < VALS.length ? VALS[ord] : null;
        }
    }
}
