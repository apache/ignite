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

package org.apache.ignite.cdc;

import java.io.Serializable;
import org.apache.ignite.internal.cdc.IgniteCDC;
import org.apache.ignite.lang.IgniteExperimental;
import org.jetbrains.annotations.Nullable;

/**
 * Event of single entry change.
 * Instance presents new value of modified entry.
 *
 * @see IgniteCDC
 * @see CaptureDataChangeConsumer
 */
@IgniteExperimental
public interface ChangeEvent extends Serializable {
    /**
     * @return Key for the changed entry.
     */
    public Object key();

    /**
     * @return Value for the changed entry or {@code null} in case of entry removal.
     */
    @Nullable public Object value();

    /**
     * @return {@code True} if event fired on primary node for partition containing this entry.
     * @see <a href="https://ignite.apache.org/docs/latest/configuring-caches/configuring-backups#configuring-partition-backups">Configuring partition backups.</a>
     */
    public boolean primary();

    /**
     * @return Partition number.
     */
    public int partition();

    /**
     * @return Order of the update operation.
     */
    public ChangeEventOrder order();

    /**
     * @return Cache ID.
     * @see org.apache.ignite.internal.util.typedef.internal.CU#cacheId(String)
     */
    public int cacheId();
}
