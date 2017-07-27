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

namespace Apache.Ignite.Core.Impl
{
    using System;
    using Apache.Ignite.Core.Impl.Binary;
    using Apache.Ignite.Core.Impl.Binary.IO;
    using Apache.Ignite.Core.Interop;

    /// <summary>
    /// Extended platform target interface.
    /// </summary>
    internal interface IPlatformTargetInternal : IPlatformTarget, IDisposable  // TODO: Verify consistent naming.
    {
        Marshaller Marshaller { get; }

        long OutOp(int type, Action<IBinaryStream> action);

        IPlatformTargetInternal OutOpObject(int type, Action<IBinaryStream> action);
        
        IPlatformTargetInternal OutOpObject(int type);

        T InOp<T>(int type, Func<IBinaryStream, T> action);

        TR DoOutInOp<TR>(int type, Action<BinaryWriter> outAction, Func<IBinaryStream, TR> inAction);

        TR DoOutInOpX<TR>(int type, Action<BinaryWriter> outAction, Func<IBinaryStream, long, TR> inAction,
            Func<IBinaryStream, Exception> inErrorAction);

        bool DoOutInOpX(int type, Action<BinaryWriter> outAction, Func<IBinaryStream, Exception> inErrorAction);

        TR DoOutInOp<TR>(int type, Action<BinaryWriter> outAction,
            Func<IBinaryStream, IPlatformTargetInternal, TR> inAction, IPlatformTargetInternal arg);
    }
}