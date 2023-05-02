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

<<<<<<<< HEAD:modules/core/src/main/java/org/apache/ignite/internal/management/cdc/CdcResendCommandArg.java
package org.apache.ignite.internal.management.cdc;
========
package org.apache.ignite.internal.management;
>>>>>>>> IGNITE-15629:modules/core/src/main/java/org/apache/ignite/internal/management/ChangeTagCommandArg.java

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import org.apache.ignite.internal.dto.IgniteDataTransferObject;
import org.apache.ignite.internal.management.api.Argument;
<<<<<<<< HEAD:modules/core/src/main/java/org/apache/ignite/internal/management/cdc/CdcResendCommandArg.java
import org.apache.ignite.internal.util.typedef.internal.U;

/** */
public class CdcResendCommandArg extends IgniteDataTransferObject {
========
import org.apache.ignite.internal.management.api.Positional;
import org.apache.ignite.internal.management.api.WithCliConfirmParameter;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.U;

/** */
@WithCliConfirmParameter
public class ChangeTagCommandArg extends IgniteDataTransferObject {
>>>>>>>> IGNITE-15629:modules/core/src/main/java/org/apache/ignite/internal/management/ChangeTagCommandArg.java
    /** */
    private static final long serialVersionUID = 0;

    /** */
<<<<<<<< HEAD:modules/core/src/main/java/org/apache/ignite/internal/management/cdc/CdcResendCommandArg.java
    @Argument(
        description = "specifies a comma-separated list of cache names",
        example = "cache1,...,cacheN"
    )
    private String[] caches;

    /** {@inheritDoc} */
    @Override protected void writeExternalData(ObjectOutput out) throws IOException {
        U.writeArray(out, caches);
========
    @Positional
    @Argument(javaStyleExample = true)
    private String newTagValue;

    /** {@inheritDoc} */
    @Override protected void writeExternalData(ObjectOutput out) throws IOException {
        U.writeString(out, newTagValue);
>>>>>>>> IGNITE-15629:modules/core/src/main/java/org/apache/ignite/internal/management/ChangeTagCommandArg.java
    }

    /** {@inheritDoc} */
    @Override protected void readExternalData(byte protoVer, ObjectInput in) throws IOException, ClassNotFoundException {
<<<<<<<< HEAD:modules/core/src/main/java/org/apache/ignite/internal/management/cdc/CdcResendCommandArg.java
        caches = U.readArray(in, String.class);
    }

    /** */
    public String[] caches() {
        return caches;
    }

    /** */
    public void caches(String[] caches) {
        this.caches = caches;
========
        newTagValue = U.readString(in);
    }

    /** */
    public String newTagValue() {
        return newTagValue;
    }

    /** */
    public void newTagValue(String newTagValue) {
        if (F.isEmpty(newTagValue))
            throw new IllegalArgumentException("newTagValue is empty.");

        this.newTagValue = newTagValue;
>>>>>>>> IGNITE-15629:modules/core/src/main/java/org/apache/ignite/internal/management/ChangeTagCommandArg.java
    }
}
