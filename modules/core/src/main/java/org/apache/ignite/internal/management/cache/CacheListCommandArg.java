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
import java.util.UUID;
import org.apache.ignite.internal.dto.IgniteDataTransferObject;
import org.apache.ignite.internal.management.api.Argument;
import org.apache.ignite.internal.management.api.ArgumentGroup;
import org.apache.ignite.internal.management.api.Positional;
import org.apache.ignite.internal.util.typedef.internal.U;

/** */
@ArgumentGroup(value = {"groups", "seq"}, onlyOneOf = true, optional = true)
public class CacheListCommandArg extends IgniteDataTransferObject {
    /** */
    private static final long serialVersionUID = 0;

    /** */
    @Positional
    @Argument(example = "regexPattern")
    private String regex;

    /** */
    @Argument(description = "print all configuration parameters for each cache", optional = true)
    private boolean config;

    /** */
    @Positional
    @Argument(optional = true, example = "nodeId")
    private UUID nodeId;

    /** */
    @Argument(description = "print configuration parameters per line. " +
        "This option has effect only when used with --config and without [--groups|--seq]",
        example = "multi-line", optional = true)
    private String outputFormat;

    /** */
    @Argument(description = "print information about groups")
    private boolean groups;

    /** */
    @Argument(description = "print information about sequences")
    private boolean seq;

    /** {@inheritDoc} */
    @Override protected void writeExternalData(ObjectOutput out) throws IOException {
        U.writeString(out, regex);
        out.writeBoolean(config);
        U.writeUuid(out, nodeId);
        U.writeString(out, outputFormat);
        out.writeBoolean(groups);
        out.writeBoolean(seq);
    }

    /** {@inheritDoc} */
    @Override protected void readExternalData(ObjectInput in) throws IOException, ClassNotFoundException {
        regex = U.readString(in);
        config = in.readBoolean();
        nodeId = U.readUuid(in);
        outputFormat = U.readString(in);
        groups = in.readBoolean();
        seq = in.readBoolean();
    }

    /** */
    public String regex() {
        return regex;
    }

    /** */
    public void regex(String regex) {
        this.regex = regex;
    }

    /** */
    public boolean groups() {
        return groups;
    }

    /** */
    public void groups(boolean groups) {
        this.groups = groups;
    }

    /** */
    public boolean seq() {
        return seq;
    }

    /** */
    public void seq(boolean seq) {
        this.seq = seq;
    }

    /** */
    public String outputFormat() {
        return outputFormat;
    }

    /** */
    public void outputFormat(String outputFormat) {
        this.outputFormat = outputFormat;
    }

    /** */
    public boolean config() {
        return config;
    }

    /** */
    public void config(boolean config) {
        this.config = config;
    }

    /** */
    public UUID nodeId() {
        return nodeId;
    }

    /** */
    public void nodeId(UUID nodeId) {
        this.nodeId = nodeId;
    }
}
