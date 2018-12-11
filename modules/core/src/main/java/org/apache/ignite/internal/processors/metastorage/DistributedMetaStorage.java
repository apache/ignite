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

package org.apache.ignite.internal.processors.metastorage;

import java.io.Serializable;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.lang.IgniteBiInClosure;
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.ignite.lang.IgnitePredicate;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/** */
public interface DistributedMetaStorage {
    /** */
    @Nullable <S extends Serializable> S read(@NotNull String key) throws IgniteCheckedException;

    /** */
    void write(@NotNull String key, @NotNull Serializable val) throws IgniteCheckedException;

    /** */
    void remove(@NotNull String key) throws IgniteCheckedException;

    /** */
    void iterate(@NotNull IgnitePredicate<String> keyPred, @NotNull IgniteInClosure<String> cb) throws IgniteCheckedException;

    /** */
    void iterate(@NotNull IgnitePredicate<String> keyPred, @NotNull IgniteBiInClosure<String, ? super Serializable> cb) throws IgniteCheckedException;

    /** */
    void listen(@NotNull String key, @NotNull IgniteBiInClosure<String, ? extends Serializable> lsnr);

    /** */
    void listen(
        @NotNull IgnitePredicate<String> keyPred,
        @NotNull IgniteBiInClosure<String, ? extends Serializable> lsnr
    );
}
