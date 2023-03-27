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

package org.apache.ignite.tests.p2p;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.io.Serializable;

/**
 * Test value for deployment.
 */
public class CacheDeploymentExternalizableTestValue implements Serializable {
    /** */
    private CacheDeploymentExternalizableTestValue2 field;

    /**
     * @return value
     */
    public CacheDeploymentExternalizableTestValue2 getField() {
        return field;
    }

    /**
     * @param field field
     */
    public void setField(
        CacheDeploymentExternalizableTestValue2 field) {
        this.field = field;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        field = new CacheDeploymentExternalizableTestValue2();

        return super.toString();
    }

    /**
     *
     */
    public static class CacheDeploymentExternalizableTestValue2 implements Externalizable {
        /** {@inheritDoc} */
        @Override public void writeExternal(ObjectOutput out) throws IOException {

        }

        /** {@inheritDoc} */
        @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {

        }
    }
}
