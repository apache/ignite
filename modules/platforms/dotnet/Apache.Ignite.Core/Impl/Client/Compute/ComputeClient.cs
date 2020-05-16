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

        /// <summary>
        /// Initializes a new instance of <see cref="ComputeClient"/>.
        /// </summary>
        internal ComputeClient(IgniteClient ignite, ComputeClientFlags flags = ComputeClientFlags.None)
        {
            // TODO: Projection, flags.
            _ignite = ignite;
            _flags = flags;
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
            var tcs = new TaskCompletionSource<TRes>();
            
            _ignite.Socket.DoOutInOp(ClientOp.ComputeTaskExecute, ctx =>
                {
                    var w = ctx.Writer;

                    w.WriteInt(0); // TODO: Projection.
                    w.WriteByte(0); // TODO: Flags
                    w.WriteLong(0); // TODO: Timeout
                    w.WriteString(taskName);
                    w.WriteObject(taskArg);
                },
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
                    
                    return taskId;
                });

            return tcs.Task;
        }

        /** <inheritdoc /> */
        public IComputeClient WithTimeout(TimeSpan timeout)
        {
            throw new NotImplementedException();
        }

        /** <inheritdoc /> */
        public IComputeClient WithNoFailover()
        {
            throw new NotImplementedException();
        }

        /** <inheritdoc /> */
        public IComputeClient WithNoResultCache()
        {
            throw new NotImplementedException();
        }
    }
}