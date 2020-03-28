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

package org.apache.ignite.spi.systemview.view;

/**
 * Implementation provides data for some internal Ignite objects.
 *
 * @param <R> Type of the row.
 */
public interface SystemView<R> extends Iterable<R> {
    /** @return Helper for exporters. */
    public SystemViewRowAttributeWalker<R> walker();

    /** @return View name. */
    public String name();

    /** @return View description. */
    public String description();

    /** @return Size of the view. */
    public int size();
}
