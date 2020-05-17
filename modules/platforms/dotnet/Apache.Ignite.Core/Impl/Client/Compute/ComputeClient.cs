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
    using System.Threading;
    using System.Threading.Tasks;
    using Apache.Ignite.Core.Client;
    using Apache.Ignite.Core.Client.Compute;
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

        public Task<TRes> ExecuteJavaTaskAsync<TRes>(string taskName, object taskArg, 
            CancellationToken cancellationToken)
        {
            IgniteArgumentCheck.NotNullOrEmpty(taskName, "taskName");
            
            var tcs = new TaskCompletionSource<TRes>(cancellationToken);
            
            var task = _ignite.Socket.DoOutInOpAsync(
                ClientOp.ComputeTaskExecute,
                ctx => ExecuteJavaTaskWrite(taskName, taskArg, ctx),
                ctx =>
                {
                    var taskId = ctx.Stream.ReadLong();

                    // TODO: Extract common logic to some method like "HandleSingleNotification".
                    // TODO: Make sure to fail the task on disconnect.
                    ctx.Socket.AddNotificationHandler(taskId, s =>
                    {
                        ctx.Socket.RemoveNotificationHandler(taskId);
                        
                        var reader = ctx.Marshaller.StartUnmarshal(s);
                        var flags = (ClientFlags) reader.ReadShort();
                        var opCode = (ClientOp) reader.ReadShort();

                        if (opCode != ClientOp.ComputeTaskFinished)
                        {
                            tcs.SetException(new IgniteClientException(
                                string.Format("Invalid server notification code. Expected {0}, but got {1}",
                                    ClientOp.ComputeTaskFinished, opCode)));
                        } 
                        else if ((flags & ClientFlags.Error) == ClientFlags.Error)
                        {
                            var status = (ClientStatusCode) reader.ReadInt();
                            var msg = reader.ReadString();
                            
                            tcs.SetException(new IgniteClientException(msg, null, status));
                        }
                        else
                        {
                            tcs.SetResult(reader.ReadObject<TRes>());
                        }
                    });

                    return taskId; // Unused.
                });

            task.ContinueWith(t => tcs.SetException(t.Exception), TaskContinuationOptions.OnlyOnFaulted);
            task.ContinueWith(t => tcs.SetCanceled(), TaskContinuationOptions.OnlyOnCanceled);

            return tcs.Task;
        }

        private void ExecuteJavaTaskWrite(string taskName, object taskArg, ClientRequestContext ctx)
        {
            var writer = ctx.Writer;

            if (_clusterGroup != null)
            {
                var nodes = _clusterGroup.GetNodes();
                writer.WriteInt(nodes.Count);

                foreach (var node in nodes)
                {
                    writer.WriteGuid(node.Id);
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
            var flags = _flags | ComputeClientFlags.NoFailover;

            return flags != _flags 
                ? new ComputeClient(_ignite, flags, _timeout, _clusterGroup) 
                : this;
        }

        /** <inheritdoc /> */
        public IComputeClient WithNoResultCache()
        {
            var flags = _flags | ComputeClientFlags.NoResultCache;

            return flags != _flags 
                ? new ComputeClient(_ignite, flags, _timeout, _clusterGroup) 
                : this;
        }
    }
}