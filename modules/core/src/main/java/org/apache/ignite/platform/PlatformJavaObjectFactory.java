/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.platform;

/**
 * Object factory used for advanced interop between platform and Java.
 * <p>
 * Use it when you need Java component for some Ignite feature in platform code. E.g. Java-based continuous
 * query filter.
 * <p>
 * You should implement the factory, compile it and then place it into node's classpath. Then you can reference
 * the factory form platform code using it's fully-qualified Java class name.
 */
public interface PlatformJavaObjectFactory<T> {
    /**
     * Constructs and returns a fully configured instance of T.
     *
     * @return An instance of T.
     */
    public T create();
}
