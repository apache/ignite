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

package org.apache.ignite.internal.commandline.property.tasks;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Collection;
import java.util.Collections;
import org.apache.ignite.internal.dto.IgniteDataTransferObject;
import org.apache.ignite.internal.processors.task.GridInternal;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteBiTuple;

/**
 * Represents information about cluster metadata.
 */
@GridInternal
public class PropertiesListResult extends IgniteDataTransferObject {
    /** */
    private static final long serialVersionUID = 0L;

    /** Properties name and description. */
    private Collection<IgniteBiTuple<String, String>> props = Collections.emptyList();

    /**
     * Constructor for optimized marshaller.
     */
    public PropertiesListResult() {
        // No-op.
    }

    /**
     * @param props Meta.
     */
    public PropertiesListResult(Collection<IgniteBiTuple<String, String>> props) {
        this.props = props;
    }

    /** {@inheritDoc} */
    @Override protected void writeExternalData(ObjectOutput out) throws IOException {
        U.writeCollection(out, props);
    }

    /** {@inheritDoc} */
    @Override protected void readExternalData(
        byte protoVer,
        ObjectInput in
    ) throws IOException, ClassNotFoundException {
        props = U.readCollection(in);
    }

    /**
     * @return Properties (name, description) collection.
     */
    public Collection<IgniteBiTuple<String, String>> properties() {
        return props;
    }
}
