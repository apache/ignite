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
import org.apache.ignite.internal.dto.IgniteDataTransferObject;
import org.apache.ignite.internal.management.api.Argument;
import org.apache.ignite.internal.management.api.ArgumentGroup;
import org.apache.ignite.internal.management.api.EnumDescription;
import org.apache.ignite.internal.management.api.Positional;
import org.apache.ignite.internal.util.typedef.internal.U;

/** */
@ArgumentGroup(value = {"caches", "allCaches"}, onlyOneOf = true, optional = false)
public class CacheMetricsCommandArg extends IgniteDataTransferObject {
    /** */
    private static final long serialVersionUID = 0;

    /** */
    @Positional
    @Argument
    @EnumDescription(
        names = {
            "ENABLE",
            "DISABLE",
            "STATUS"
        },
        descriptions = {
            "Enable metrics",
            "Disable metrics",
            "Status"
        }
    )
    private CacheMetricsOperation operation;

    /** */
    @Argument(description = "specifies a comma-separated list of cache names to which operation should be applied",
        example = "cache1[,...,cacheN]")
    private String[] caches;

    /** */
    @Argument(description = "applies operation to all user caches")
    private boolean allCaches;

    /** {@inheritDoc} */
    @Override protected void writeExternalData(ObjectOutput out) throws IOException {
        U.writeEnum(out, operation);
        U.writeArray(out, caches);
        out.writeBoolean(allCaches);
    }

    /** {@inheritDoc} */
    @Override protected void readExternalData(byte protoVer, ObjectInput in) throws IOException, ClassNotFoundException {
        operation = U.readEnum(in, CacheMetricsOperation.class);
        caches = U.readArray(in, String.class);
        allCaches = in.readBoolean();
    }

    /** */
    public CacheMetricsOperation operation() {
        return operation;
    }

    /** */
    public void operation(CacheMetricsOperation op) {
        this.operation = op;
    }

    /** */
    public String[] caches() {
        return caches;
    }

    /** */
    public void caches(String[] caches) {
        this.caches = caches;
    }

    /** */
    public boolean allCaches() {
        return allCaches;
    }

    /** */
    public void allCaches(boolean allCaches) {
        this.allCaches = allCaches;
    }
}
