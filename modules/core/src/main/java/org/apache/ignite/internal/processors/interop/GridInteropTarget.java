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

package org.apache.ignite.internal.processors.interop;

import org.apache.ignite.*;
import org.jetbrains.annotations.*;

/**
 * Interop target abstraction.
 */
public interface GridInteropTarget {
    /**
     * Synchronous IN operation.
     *
     * @param type Operation type.
     * @param stream Input stream.
     * @return Value specific for the given operation otherwise.
     * @throws IgniteCheckedException In case of failure.
     */
    public int inOp(int type, GridPortableInputStream stream) throws IgniteCheckedException;

    /**
     * Synchronous IN operation which returns managed object as result.
     *
     * @param type Operation type.
     * @param stream Input stream.
     * @return Managed result.
     * @throws IgniteCheckedException If case of failure.
     */
    public Object inOpObject(int type, GridPortableInputStream stream) throws IgniteCheckedException;

    /**
     * Synchronous OUT operation.
     *
     * @param type Operation type.
     * @param stream Native stream address.
     * @param arr Native array address.
     * @param cap Capacity.
     * @throws IgniteCheckedException In case of failure.
     */
    public void outOp(int type, long stream, long arr, int cap) throws IgniteCheckedException;

    /**
     * Synchronous IN-OUT operation.
     *
     * @param type Operation type.
     * @param inStream Input stream.
     * @param outStream Native stream address.
     * @param outArr Native array address.
     * @param outCap Capacity.
     * @throws IgniteCheckedException In case of failure.
     */
    public void inOutOp(int type, GridPortableInputStream inStream, long outStream, long outArr, int outCap)
        throws IgniteCheckedException;

    /**
     * Synchronous IN-OUT operation with optional argument.
     *
     * @param type Operation type.
     * @param inStream Input stream.
     * @param outStream Native stream address.
     * @param outArr Native array address.
     * @param outCap Capacity.
     * @param arg Argument (optional).
     * @throws IgniteCheckedException In case of failure.
     */
    public void inOutOp(int type, GridPortableInputStream inStream, long outStream, long outArr, int outCap,
        @Nullable Object arg) throws IgniteCheckedException;

    /**
     * Asynchronous IN operation.
     *
     * @param type Operation type.
     * @param futId Future ID.
     * @param in Input stream.
     * @throws IgniteCheckedException In case of failure.
     */
    public void inOpAsync(int type, long futId, GridPortableInputStream in) throws IgniteCheckedException;

    /**
     * Asynchronous IN-OUT operation.
     *
     * @param type Operation type.
     * @param futId Future ID.
     * @param in Input stream.
     * @param outStream Native stream address.
     * @param outArr Native array address.
     * @param outCap Capacity.
     * @throws IgniteCheckedException In case of failure.
     */
    public void inOutOpAsync(int type, long futId, GridPortableInputStream in, long outStream, long outArr, int outCap)
        throws IgniteCheckedException;
}
