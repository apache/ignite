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

package org.apache.ignite.internal.configuration;

import org.apache.ignite.configuration.ConfigurationTree;
import org.apache.ignite.configuration.DirectConfigurationProperty;

/**
 * {@link ConfigurationTree} wrapper with {@link DirectConfigurationProperty}.
 *
 * @param <VIEWT>   Value type of the node.
 * @param <CHANGET> Type of the object that changes this node's value.
 */
public class DirectConfigurationTreeWrapper<VIEWT, CHANGET> extends ConfigurationTreeWrapper<VIEWT, CHANGET>
        implements DirectConfigurationProperty<VIEWT> {
    /**
     * Constructor.
     *
     * @param configTree Configuration tree.
     */
    public DirectConfigurationTreeWrapper(ConfigurationTree<VIEWT, CHANGET> configTree) {
        super(configTree);
        
        assert configTree instanceof DirectConfigurationProperty : configTree;
    }
    
    /** {@inheritDoc} */
    @Override
    public VIEWT directValue() {
        return ((DirectConfigurationProperty<VIEWT>) configTree).directValue();
    }
}
