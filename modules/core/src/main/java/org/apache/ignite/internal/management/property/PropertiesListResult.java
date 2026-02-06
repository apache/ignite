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

package org.apache.ignite.internal.management.property;

import java.util.Collection;
import org.apache.ignite.internal.Order;
import org.apache.ignite.internal.dto.IgniteDataTransferObject;
import org.apache.ignite.internal.processors.task.GridInternal;
import org.apache.ignite.internal.util.typedef.T3;

/**
 * List of the distributed properties names.
 */
@GridInternal
public class PropertiesListResult extends IgniteDataTransferObject {
    /** */
    private static final long serialVersionUID = 0L;

    /** Properties info: name, value, description. */
    @Order(value = 0)
    Collection<T3<String, String, String>> props;

    /**
     * Constructor for optimized marshaller.
     */
    public PropertiesListResult() {
        // No-op.
    }

    /**
     * @param props Properties.
     */
    public PropertiesListResult(Collection<T3<String, String, String>> props) {
        this.props = props;
    }

    /**
     * @return Properties (name, value, description) collection.
     */
    public Collection<T3<String, String, String>> properties() {
        return props;
    }
}
