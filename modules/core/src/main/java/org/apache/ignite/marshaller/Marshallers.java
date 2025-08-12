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

package org.apache.ignite.marshaller;

import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.marshaller.jdk.JdkMarshaller;
import org.apache.ignite.marshaller.jdk.JdkMarshallerImpl;
import org.jetbrains.annotations.Nullable;

/**
 * Factory to create implementation of {@link Marshaller}.
 */
public class Marshallers {
    /** Singleton instance. */
    private static final JdkMarshaller INSTANCE = new JdkMarshallerImpl();

    /** @return Default instance of {@link JdkMarshaller}. */
    public static JdkMarshaller jdkMarshaller() {
        return INSTANCE;
    }

    /**
     * @param clsFilter Class filter.
     * @return Filtered instance of {@link JdkMarshaller}.
     */
    public static JdkMarshaller jdkMarshaller(@Nullable IgnitePredicate<String> clsFilter) {
        return new JdkMarshallerImpl(clsFilter);
    }
}
