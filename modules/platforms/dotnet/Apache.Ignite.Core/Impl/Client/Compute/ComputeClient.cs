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

namespace Apache.Ignite.Core.Impl.Client.Compute
{
    using System;
    using System.Diagnostics.CodeAnalysis;
    using System.Threading;
    using System.Threading.Tasks;
    using Apache.Ignite.Core.Client;
    using Apache.Ignite.Core.Client.Compute;
    using Apache.Ignite.Core.Impl.Binary;
    using Apache.Ignite.Core.Impl.Common;

    /// <summary>
    /// Compute client.
    /// </summary>
    internal class ComputeClient : IComputeClient
    {
        /** */
        private readonly IgniteClient _ignite;

        /** */
        private readonly ComputeClientFlags _flags;

        /** */
        private readonly TimeSpan _timeout;

        /** */
        private readonly IClientClusterGroup _clusterGroup;

        /// <summary>
        /// Initializes a new instance of <see cref="ComputeClient"/>.
        /// </summary>
        internal ComputeClient(
            IgniteClient ignite,
            ComputeClientFlags flags,
            TimeSpan? timeout,
            IClientClusterGroup clusterGroup)
        {
            _ignite = ignite;
            _flags = flags;
            _timeout = timeout ?? TimeSpan.Zero;
            _clusterGroup = clusterGroup;
        }

        /** <inheritdoc /> */
        public TRes ExecuteJavaTask<TRes>(string taskName, object taskArg)
        {
            IgniteArgumentCheck.NotNullOrEmpty(taskName, "taskName");

            return ExecuteJavaTaskAsync<TRes>(taskName, taskArg).Result;
        }

        /** <inheritdoc /> */
        public Task<TRes> ExecuteJavaTaskAsync<TRes>(string taskName, object taskArg)
        {
            return ExecuteJavaTaskAsync<TRes>(taskName, taskArg, CancellationToken.None);
        }

        /** <inheritdoc /> */
        public Task<TRes> ExecuteJavaTaskAsync<TRes>(string taskName, object taskArg,
            CancellationToken cancellationToken)
        {
            IgniteArgumentCheck.NotNullOrEmpty(taskName, "taskName");

            var tcs = new TaskCompletionSource<TRes>();
            cancellationToken.Register(() => tcs.TrySetCanceled());

            var keepBinary = (_flags & ComputeClientFlags.KeepBinary) == ComputeClientFlags.KeepBinary;

            var task = _ignite.Socket.DoOutInOpAsync(
                ClientOp.ComputeTaskExecute,
                ctx => WriteJavaTaskRequest(ctx, taskName, taskArg),
                ctx => ReadJavaTaskResponse(ctx, tcs, cancellationToken, keepBinary));

            // ReSharper disable once AssignNullToNotNullAttribute (t.Exception won't be null).
            task.ContinueWith(t => tcs.TrySetException(t.Exception), TaskContinuationOptions.OnlyOnFaulted);
            task.ContinueWith(t => tcs.TrySetCanceled(), TaskContinuationOptions.OnlyOnCanceled);

            return tcs.Task;
        }

        /** <inheritdoc /> */
        public IComputeClient WithTimeout(TimeSpan timeout)
        {
            return _timeout != timeout
                ? new ComputeClient(_ignite, _flags, timeout, _clusterGroup)
                : this;
        }

        /** <inheritdoc /> */
        public IComputeClient WithNoFailover()
        {
            return SetFlag(ComputeClientFlags.NoFailover);
        }

        /** <inheritdoc /> */
        public IComputeClient WithNoResultCache()
        {
            return SetFlag(ComputeClientFlags.NoResultCache);
        }

        /** <inheritdoc /> */
        public IComputeClient WithKeepBinary()
        {
            return SetFlag(ComputeClientFlags.KeepBinary);
        }

        /// <summary>
        /// Returns a new instance with the given flag enabled, or this instance if the flag is already present.
        /// </summary>
        private IComputeClient SetFlag(ComputeClientFlags newFlag)
        {
            var flags = _flags | newFlag;

            return flags != _flags
                ? new ComputeClient(_ignite, flags, _timeout, _clusterGroup)
                : this;
        }

        /// <summary>
        /// Writes the java task.
        /// </summary>
        private void WriteJavaTaskRequest(ClientRequestContext ctx, string taskName, object taskArg)
        {
            var writer = ctx.Writer;

            if (_clusterGroup != null)
            {
                var nodes = _clusterGroup.GetNodes();
                writer.WriteInt(nodes.Count);

                foreach (var node in nodes)
                {
                    BinaryUtils.WriteGuid(node.Id, ctx.Stream);
                }
            }
            else
            {
                writer.WriteInt(0);
            }

            writer.WriteByte((byte) _flags);
            writer.WriteLong((long) _timeout.TotalMilliseconds);
            writer.WriteString(taskName);
            writer.WriteObject(taskArg);

            ctx.Socket.ExpectNotifications();
        }

        /// <summary>
        /// Reads java task execution response.
        /// </summary>
        [SuppressMessage("Microsoft.Design", "CA1031:DoNotCatchGeneralExceptionTypes")]
        private static object ReadJavaTaskResponse<TRes>(ClientResponseContext ctx, TaskCompletionSource<TRes> tcs,
            CancellationToken cancellationToken, bool keepBinary)
        {
            var taskId = ctx.Stream.ReadLong();

            cancellationToken.Register(() =>
            {
                ctx.Socket.DoOutInOpAsync<object>(ClientOp.ResourceClose,
                    c => c.Stream.WriteLong(taskId),
                    _ => null,
                    (status, msg) =>
                    {
                        if (status == ClientStatusCode.ResourceDoesNotExist)
                        {
                            // Task finished before we could cancel it - ignore.
                            return null;
                        }

                        throw new IgniteClientException(msg, null, status);
                    });
            });

            ctx.Socket.AddNotificationHandler(taskId, (stream, ex) =>
            {
                ctx.Socket.RemoveNotificationHandler(taskId);

                if (ex != null)
                {
                    tcs.TrySetException(ex);
                    return;
                }

                var reader = ctx.Marshaller.StartUnmarshal(stream,
                    keepBinary ? BinaryMode.ForceBinary : BinaryMode.Deserialize);

                try
                {
                    var flags = (ClientFlags) reader.ReadShort();
                    var opCode = (ClientOp) reader.ReadShort();

                    if (opCode != ClientOp.ComputeTaskFinished)
                    {
                        tcs.TrySetException(new IgniteClientException(
                            string.Format("Invalid server notification code. Expected {0}, but got {1}",
                                ClientOp.ComputeTaskFinished, opCode)));
                    }
                    else if ((flags & ClientFlags.Error) == ClientFlags.Error)
                    {
                        var status = (ClientStatusCode) reader.ReadInt();
                        var msg = reader.ReadString();

                        tcs.TrySetException(new IgniteClientException(msg, null, status));
                    }
                    else
                    {
                        tcs.TrySetResult(reader.ReadObject<TRes>());
                    }
                }
                catch (Exception e)
                {
                    tcs.TrySetException(e);
                }
            });

            return null;
        }
    }
}
