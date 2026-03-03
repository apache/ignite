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

package org.apache.ignite.internal.binary.builder;

import java.util.Iterator;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.binary.BinaryObjectBuilder;
import org.apache.ignite.internal.binary.BinaryContext;
import org.apache.ignite.internal.util.CommonUtils;
import org.apache.ignite.internal.util.typedef.internal.A;

/**
 * Utility class to provide static methods to create {@link BinaryObjectBuilder}.
 */
public class BinaryObjectBuilders {
    /** Streams factory implementation. */
    private static final BinaryObjectBuildersFactory factory;

    static {
        Iterator<BinaryObjectBuildersFactory> factories = CommonUtils.loadService(BinaryObjectBuildersFactory.class).iterator();

        A.ensure(
            factories.hasNext(),
            "Implementation for BinaryObjectBuildersFactory service not found. Please add ignite-binary-impl to classpath"
        );

        factory = factories.next();
    }

    /**
     * @param obj Object to convert to builder.
     * @return Builder instance.
     */
    public static BinaryObjectBuilder builder(BinaryObject obj) {
        return factory.builder(obj);
    }

    /**
     * @param binaryCtx Binary context.
     * @param clsName Class name.
     * @return Builder instance.
     */
    public static BinaryObjectBuilder builder(BinaryContext binaryCtx, String clsName) {
        return factory.builder(binaryCtx, clsName);
    }
}
