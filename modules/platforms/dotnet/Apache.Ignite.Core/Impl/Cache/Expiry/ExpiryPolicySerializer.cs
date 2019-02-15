/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
 */

namespace Apache.Ignite.Core.Impl.Cache.Expiry
{
    using System;
    using System.Diagnostics;
    using Apache.Ignite.Core.Binary;
    using Apache.Ignite.Core.Cache.Expiry;
    using Apache.Ignite.Core.Common;

    /// <summary>
    /// Writer for <see cref="IExpiryPolicy"/>.
    /// </summary>
    internal static class ExpiryPolicySerializer
    {
        /** Duration: unchanged. */
        private const long DurUnchanged = -2;

        /** Duration: eternal. */
        private const long DurEternal = -1;

        /** Duration: zero. */
        private const long DurZero = 0;

        /// <summary>
        /// Writes the policy.
        /// </summary>
        public static void WritePolicy(IBinaryRawWriter writer, IExpiryPolicy plc)
        {
            Debug.Assert(plc != null);
            Debug.Assert(writer != null);

            writer.WriteLong(ConvertDuration(plc.GetExpiryForCreate()));
            writer.WriteLong(ConvertDuration(plc.GetExpiryForUpdate()));
            writer.WriteLong(ConvertDuration(plc.GetExpiryForAccess()));
        }

        /// <summary>
        /// Reads the policy.
        /// </summary>
        public static IExpiryPolicy ReadPolicy(IBinaryRawReader reader)
        {
            return new ExpiryPolicy(ConvertDuration(reader.ReadLong()), ConvertDuration(reader.ReadLong()),
                ConvertDuration(reader.ReadLong()));
        }

        /// <summary>
        /// Writes the policy factory.
        /// </summary>
        public static void WritePolicyFactory(IBinaryRawWriter writer, IFactory<IExpiryPolicy> factory)
        {
            Debug.Assert(writer != null);

            if (factory != null)
            {
                writer.WriteBoolean(true);
                var expiryPolicy = factory.CreateInstance();

                if (expiryPolicy == null)
                    throw new IgniteException("ExpiryPolicyFactory should return non-null result.");

                WritePolicy(writer, expiryPolicy);
            }
            else
                writer.WriteBoolean(false);
        }

        /// <summary>
        /// Reads the expiry policy factory.
        /// </summary>
        public static IFactory<IExpiryPolicy> ReadPolicyFactory(IBinaryRawReader reader)
        {
            return reader.ReadBoolean() ? new ExpiryPolicyFactory(ReadPolicy(reader)) : null;
        }

        /// <summary>
        /// Convert TimeSpan to duration recognizable by Java.
        /// </summary>
        /// <param name="dur">.NET duration.</param>
        /// <returns>Java duration in milliseconds.</returns>
        private static long ConvertDuration(TimeSpan? dur)
        {
            if (dur.HasValue)
            {
                if (dur.Value == TimeSpan.MaxValue)
                    return DurEternal;

                long dur0 = (long) dur.Value.TotalMilliseconds;

                return dur0 > 0 ? dur0 : DurZero;
            }

            return DurUnchanged;
        }

        /// <summary>
        /// Convert duration recognizable by Java to TimeSpan.
        /// </summary>
        /// <param name="dur">Java duration.</param>
        /// <returns>.NET duration.</returns>
        private static TimeSpan? ConvertDuration(long dur)
        {
            switch (dur)
            {
                case DurUnchanged:
                    return null;

                case DurEternal:
                    return TimeSpan.MaxValue;

                case DurZero:
                    return TimeSpan.Zero;

                default:
                    return TimeSpan.FromMilliseconds(dur);
            }
        }
    }
}
