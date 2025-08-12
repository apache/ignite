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

import java.util.ServiceLoader;
import org.apache.ignite.internal.marshaller.optimized.OptimizedMarshaller;
import org.apache.ignite.internal.util.CommonUtils;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.marshaller.jdk.JdkMarshaller;
import org.jetbrains.annotations.Nullable;

/**
 * Marshallers factory.
 * Implementation loaded via {@link ServiceLoader} mechanism.
 *
 * @see CommonUtils#loadService(Class)
 * @see JdkMarshaller
 * @see OptimizedMarshaller
 */
public interface MarshallersFactory {
    /** @return Default instance of {@link JdkMarshaller}. */
    public JdkMarshaller jdk();

    /**
     * @param clsFilter Class filter.
     * @return Filtered instance of {@link JdkMarshaller}.
     */
    public JdkMarshaller jdk(@Nullable IgnitePredicate<String> clsFilter);
}
