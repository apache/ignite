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

namespace Apache.Ignite.Core.Impl.Deployment
{
    using System;
    using Apache.Ignite.Core.Deployment;
    using Apache.Ignite.Core.Impl.Binary;

    /// <summary>
    /// Reader and Writer extensions for peer deployment.
    /// </summary>
    internal static class PeerLoadingExtensions
    {
        /** */
        private static readonly Func<object, PeerLoadingObjectHolder> WrapperFunc =
            x => new PeerLoadingObjectHolder(x);

        /// <summary>
        /// Writes the object with peer deployment (when enabled) or normally otherwise.
        /// </summary>
        public static void WriteWithPeerDeployment(this BinaryWriter writer, object o)
        {
            if (writer.Marshaller.IsPeerAssemblyLoadingEnabled())
            {
                try
                {
                    writer.WrapperFunc = WrapperFunc;
                    writer.WriteObjectDetached(o);
                }
                finally
                {
                    writer.WrapperFunc = null;
                }
            }
            else
            {
                writer.WriteObjectDetached(o);
            }
        }

        /// <summary>
        /// Determines whether peer loading is enabled.
        /// </summary>
        private static bool IsPeerAssemblyLoadingEnabled(this Marshaller marshaller)
        {
            return marshaller != null && marshaller.Ignite != null &&
                   marshaller.Ignite.Configuration.PeerAssemblyLoadingMode != PeerAssemblyLoadingMode.Disabled;
        }
    }
}
