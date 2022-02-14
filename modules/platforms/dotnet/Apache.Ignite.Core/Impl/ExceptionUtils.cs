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
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.Diagnostics.CodeAnalysis;
    using System.Security;
    using System.Text.RegularExpressions;
    using System.Threading;
    using Apache.Ignite.Core.Binary;
    using Apache.Ignite.Core.Cache;
    using Apache.Ignite.Core.Cache.Store;
    using Apache.Ignite.Core.Cluster;
    using Apache.Ignite.Core.Common;
    using Apache.Ignite.Core.Compute;
    using Apache.Ignite.Core.Impl.Binary;
    using Apache.Ignite.Core.Services;
    using Apache.Ignite.Core.Transactions;

    /// <summary>
    /// Managed environment. Acts as a gateway for native code.
    /// </summary>
    internal static class ExceptionUtils
    {
        /** NoClassDefFoundError fully-qualified class name which is important during startup phase. */
        private const string ClsNoClsDefFoundErr = "java.lang.NoClassDefFoundError";

        /** NoSuchMethodError fully-qualified class name which is important during startup phase. */
        private const string ClsNoSuchMthdErr = "java.lang.NoSuchMethodError";

        /** InteropCachePartialUpdateException. */
        private const string ClsCachePartialUpdateErr = "org.apache.ignite.internal.processors.platform.cache.PlatformCachePartialUpdateException";

        /** Map with predefined exceptions. */
        private static readonly IDictionary<string, ExceptionFactory> Exs = new Dictionary<string, ExceptionFactory>();

        /** Inner class regex. */
        private static readonly Regex InnerClassRegex = new Regex(@"class ([^\s]+): (.*)", RegexOptions.Compiled);

        /// <summary>
        /// Static initializer.
        /// </summary>
        [SuppressMessage("Microsoft.Performance", "CA1810:InitializeReferenceTypeStaticFieldsInline",
            Justification = "Readability")]
        static ExceptionUtils()
        {
            // Common Java exceptions mapped to common .NET exceptions.
            Exs["java.lang.IllegalArgumentException"] = (c, m, e, i) => new ArgumentException(m, e);
            Exs["java.lang.IllegalStateException"] = (c, m, e, i) => new InvalidOperationException(m, e);
            Exs["java.lang.UnsupportedOperationException"] = (c, m, e, i) => new NotSupportedException(m, e);
            Exs["java.lang.InterruptedException"] = (c, m, e, i) => new ThreadInterruptedException(m, e);
            Exs["java.lang.IllegalMonitorStateException"] = (c, m, e, i) => new SynchronizationLockException(m, e);

            // Generic Ignite exceptions.
            Exs["org.apache.ignite.IgniteException"] = (c, m, e, i) => new IgniteException(m, e);
            Exs["org.apache.ignite.IgniteCheckedException"] = (c, m, e, i) => new IgniteException(m, e);
            Exs["org.apache.ignite.IgniteIllegalStateException"] = (c, m, e, i) => new IgniteIllegalStateException(m, e);
            Exs["org.apache.ignite.IgniteClientDisconnectedException"] = (c, m, e, i) => new ClientDisconnectedException(m, e, i.GetCluster().ClientReconnectTask);
            Exs["org.apache.ignite.internal.IgniteClientDisconnectedCheckedException"] = (c, m, e, i) => new ClientDisconnectedException(m, e, i.GetCluster().ClientReconnectTask);
            Exs["org.apache.ignite.binary.BinaryObjectException"] = (c, m, e, i) => new BinaryObjectException(m, e);

            // Cluster exceptions.
            Exs["org.apache.ignite.cluster.ClusterGroupEmptyException"] = (c, m, e, i) => new ClusterGroupEmptyException(m, e);
            Exs["org.apache.ignite.internal.cluster.ClusterGroupEmptyCheckedException"] = (c, m, e, i) => new ClusterGroupEmptyException(m, e);
            Exs["org.apache.ignite.cluster.ClusterTopologyException"] = (c, m, e, i) => new ClusterTopologyException(m, e);

            // Compute exceptions.
            Exs["org.apache.ignite.compute.ComputeExecutionRejectedException"] = (c, m, e, i) => new ComputeExecutionRejectedException(m, e);
            Exs["org.apache.ignite.compute.ComputeJobFailoverException"] = (c, m, e, i) => new ComputeJobFailoverException(m, e);
            Exs["org.apache.ignite.compute.ComputeTaskCancelledException"] = (c, m, e, i) => new ComputeTaskCancelledException(m, e);
            Exs["org.apache.ignite.compute.ComputeTaskTimeoutException"] = (c, m, e, i) => new ComputeTaskTimeoutException(m, e);
            Exs["org.apache.ignite.compute.ComputeUserUndeclaredException"] = (c, m, e, i) => new ComputeUserUndeclaredException(m, e);

            // Cache exceptions.
            Exs["javax.cache.CacheException"] = (c, m, e, i) => new CacheException(m, e);
            Exs["javax.cache.integration.CacheLoaderException"] = (c, m, e, i) => new CacheStoreException(m, e);
            Exs["javax.cache.integration.CacheWriterException"] = (c, m, e, i) => new CacheStoreException(m, e);
            Exs["javax.cache.processor.EntryProcessorException"] = (c, m, e, i) => new CacheEntryProcessorException(m, e);

            // Transaction exceptions.
            Exs["org.apache.ignite.transactions.TransactionOptimisticException"] = (c, m, e, i) => new TransactionOptimisticException(m, e);
            Exs["org.apache.ignite.internal.transactions.IgniteTxOptimisticCheckedException"] = (c, m, e, i) => new TransactionOptimisticException(m, e);
            Exs["org.apache.ignite.transactions.TransactionTimeoutException"] = (c, m, e, i) => new TransactionTimeoutException(m, e);
            Exs["org.apache.ignite.transactions.TransactionRollbackException"] = (c, m, e, i) => new TransactionRollbackException(m, e);
            Exs["org.apache.ignite.transactions.TransactionHeuristicException"] = (c, m, e, i) => new TransactionHeuristicException(m, e);
            Exs["org.apache.ignite.transactions.TransactionDeadlockException"] = (c, m, e, i) => new TransactionDeadlockException(m, e);

            // Security exceptions.
            Exs["org.apache.ignite.IgniteAuthenticationException"] = (c, m, e, i) => new SecurityException(m, e);
            Exs["org.apache.ignite.plugin.security.GridSecurityException"] = (c, m, e, i) => new SecurityException(m, e);

            // Future exceptions.
            Exs["org.apache.ignite.lang.IgniteFutureCancelledException"] = (c, m, e, i) => new IgniteFutureCancelledException(m, e);
            Exs["org.apache.ignite.internal.IgniteFutureCancelledCheckedException"] = (c, m, e, i) => new IgniteFutureCancelledException(m, e);

            // Service exceptions.
            Exs["org.apache.ignite.services.ServiceDeploymentException"] = (c, m, e, i) => new ServiceDeploymentException(m, e);
        }

        /// <summary>
        /// Creates exception according to native code class and message.
        /// </summary>
        /// <param name="igniteInt">The ignite.</param>
        /// <param name="clsName">Exception class name.</param>
        /// <param name="msg">Exception message.</param>
        /// <param name="stackTrace">Native stack trace.</param>
        /// <param name="reader">Error data reader.</param>
        /// <param name="innerException">Inner exception.</param>
        /// <returns>Exception.</returns>
        public static Exception GetException(IIgniteInternal igniteInt, string clsName, string msg, string stackTrace,
            BinaryReader reader = null, Exception innerException = null)
        {
            // Set JavaException as immediate inner.
            var jex = new JavaException(clsName, msg, stackTrace, innerException);

            return GetException(igniteInt, jex, reader);
        }

        /// <summary>
        /// Creates exception according to native code class and message.
        /// </summary>
        /// <param name="igniteInt">The ignite.</param>
        /// <param name="innerException">Java exception.</param>
        /// <param name="reader">Error data reader.</param>
        /// <returns>Exception.</returns>
        public static Exception GetException(IIgniteInternal igniteInt, JavaException innerException,
            BinaryReader reader = null)
        {
            var ignite = igniteInt == null ? null : igniteInt.GetIgnite();

            var msg = innerException.JavaMessage;
            var clsName = innerException.JavaClassName;

            ExceptionFactory ctor;

            if (Exs.TryGetValue(clsName, out ctor))
            {
                var match = InnerClassRegex.Match(msg ?? string.Empty);

                ExceptionFactory innerCtor;

                if (match.Success && Exs.TryGetValue(match.Groups[1].Value, out innerCtor))
                {
                    return ctor(clsName, msg,
                        innerCtor(match.Groups[1].Value, match.Groups[2].Value, innerException, ignite),
                        ignite);
                }

                return ctor(clsName, msg, innerException, ignite);
            }

            if (ClsNoClsDefFoundErr.Equals(clsName, StringComparison.OrdinalIgnoreCase))
                return new IgniteException("Java class is not found (did you set IGNITE_HOME environment " +
                    "variable?): " + msg, innerException);

            if (ClsNoSuchMthdErr.Equals(clsName, StringComparison.OrdinalIgnoreCase))
                return new IgniteException("Java class method is not found (did you set IGNITE_HOME environment " +
                    "variable?): " + msg, innerException);

            if (ClsCachePartialUpdateErr.Equals(clsName, StringComparison.OrdinalIgnoreCase))
                return ProcessCachePartialUpdateException(igniteInt, msg, innerException.Message, reader);

            // Predefined mapping not found - check plugins.
            if (igniteInt != null && igniteInt.PluginProcessor != null)
            {
                ctor = igniteInt.PluginProcessor.GetExceptionMapping(clsName);

                if (ctor != null)
                {
                    return ctor(clsName, msg, innerException, ignite);
                }
            }

            // Return default exception.
            return new IgniteException(string.Format("Java exception occurred [class={0}, message={1}]", clsName, msg),
                innerException);
        }

        /// <summary>
        /// Process cache partial update exception.
        /// </summary>
        /// <param name="ignite">The ignite.</param>
        /// <param name="msg">Message.</param>
        /// <param name="stackTrace">Stack trace.</param>
        /// <param name="reader">Reader.</param>
        /// <returns>CachePartialUpdateException.</returns>
        [SuppressMessage("Microsoft.Design", "CA1031:DoNotCatchGeneralExceptionTypes")]
        private static Exception ProcessCachePartialUpdateException(IIgniteInternal ignite, string msg,
            string stackTrace, BinaryReader reader)
        {
            if (reader == null)
                return new CachePartialUpdateException(msg, new IgniteException("Failed keys are not available."));

            bool dataExists = reader.ReadBoolean();

            Debug.Assert(dataExists);

            if (reader.ReadBoolean())
            {
                bool keepBinary = reader.ReadBoolean();

                BinaryReader keysReader = reader.Marshaller.StartUnmarshal(reader.Stream, keepBinary);

                try
                {
                    return new CachePartialUpdateException(msg, ReadNullableList(keysReader));
                }
                catch (Exception e)
                {
                    // Failed to deserialize data.
                    return new CachePartialUpdateException(msg, e);
                }
            }

            // Was not able to write keys.
            string innerErrCls = reader.ReadString();
            string innerErrMsg = reader.ReadString();

            Exception innerErr = GetException(ignite, innerErrCls, innerErrMsg, stackTrace);

            return new CachePartialUpdateException(msg, innerErr);
        }

        /// <summary>
        /// Create JVM initialization exception.
        /// </summary>
        /// <param name="clsName">Class name.</param>
        /// <param name="msg">Message.</param>
        /// <param name="stackTrace">Stack trace.</param>
        /// <returns>Exception.</returns>
        [ExcludeFromCodeCoverage]  // Covered by a test in a separate process.
        public static Exception GetJvmInitializeException(string clsName, string msg, string stackTrace)
        {
            if (clsName != null)
                return new IgniteException("Failed to initialize JVM.", GetException(null, clsName, msg, stackTrace));

            if (msg != null)
                return new IgniteException("Failed to initialize JVM: " + msg + "\n" + stackTrace);

            return new IgniteException("Failed to initialize JVM.");
        }

        /// <summary>
        /// Reads nullable list.
        /// </summary>
        /// <param name="reader">Reader.</param>
        /// <returns>List.</returns>
        private static List<object> ReadNullableList(BinaryReader reader)
        {
            if (!reader.ReadBoolean())
                return null;

            var size = reader.ReadInt();

            var list = new List<object>(size);

            for (int i = 0; i < size; i++)
                list.Add(reader.ReadObject<object>());

            return list;
        }
    }
}
