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

package org.apache.ignite.configuration;

import java.util.ArrayList;
import java.util.Collection;
import org.apache.ignite.binary.BinaryIdMapper;
import org.apache.ignite.binary.BinarySerializer;
import org.apache.ignite.binary.BinaryTypeConfiguration;

/**
 *
 */
public class BinaryConfiguration {
    /** ID mapper. */
    private BinaryIdMapper idMapper;

    /** Serializer. */
    private BinarySerializer serializer;

    /** Types. */
    private Collection<BinaryTypeConfiguration> typeCfgs;

    /**
     * Sets class names of portable objects explicitly.
     *
     * @param clsNames Class names.
     */
    public void setClassNames(Collection<String> clsNames) {
        if (typeCfgs == null)
            typeCfgs = new ArrayList<>(clsNames.size());

        for (String clsName : clsNames)
            typeCfgs.add(new BinaryTypeConfiguration(clsName));
    }

    /**
     * Gets ID mapper.
     *
     * @return ID mapper.
     */
    public BinaryIdMapper getIdMapper() {
        return idMapper;
    }

    /**
     * Sets ID mapper.
     *
     * @param idMapper ID mapper.
     */
    public void setIdMapper(BinaryIdMapper idMapper) {
        this.idMapper = idMapper;
    }

    /**
     * Gets serializer.
     *
     * @return Serializer.
     */
    public BinarySerializer getSerializer() {
        return serializer;
    }

    /**
     * Sets serializer.
     *
     * @param serializer Serializer.
     */
    public void setSerializer(BinarySerializer serializer) {
        this.serializer = serializer;
    }

    /**
     * Gets types configuration.
     *
     * @return Types configuration.
     */
    public Collection<BinaryTypeConfiguration> getTypeConfigurations() {
        return typeCfgs;
    }

    /**
     * Sets type configurations.
     *
     * @param typeCfgs Type configurations.
     */
    public void setTypeConfigurations(Collection<BinaryTypeConfiguration> typeCfgs) {
        this.typeCfgs = typeCfgs;
    }
}
