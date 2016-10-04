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

namespace Apache.Ignite.Core.Tests.Plugin
{
    using System;
    using Apache.Ignite.Core.Impl.Common;
    using Apache.Ignite.Core.Plugin;

    /// <summary>
    /// Test Ignite plugin.
    /// </summary>
    public class TestPlugin : IDisposable
    {
        /** Target. */
        private readonly IPluginTarget _target;

        /** */
        private const int OpReadWrite = 1;

        /** */
        private const int OpError = 2;

        /** */
        private const int OpInvokeCallback = 3;

        /** */
        private const int OpGetName = 4;

        /** */
        private const int OpGetChild = 5;

        /** */
        private const int OpGetObjectName = 6;

        /** */
        private const int OpGetNodeId = 7;

        /** */
        private const int OpGetCallbackResponse = 8;

        /// <summary>
        /// Initializes a new instance of the <see cref="TestPlugin"/> class.
        /// </summary>
        /// <param name="target">The target.</param>
        public TestPlugin(IPluginTarget target)
        {
            IgniteArgumentCheck.NotNull(target, "target");

            _target = target;
        }

        /// <summary>
        /// Performs read-write operation.
        /// </summary>
        public object ReadWrite(object obj)
        {
            return _target.InvokeOperation(OpReadWrite, w => w.WriteObject(obj), 
                (r, _) => r.ReadObject<object>(), null);
        }

        /// <summary>
        /// Performs operation with error.
        /// </summary>
        public void Error(string text)
        {
            _target.InvokeOperation<object>(OpError, w => w.WriteString(text), null, null);
        }

        /// <summary>
        /// Invokes the callback.
        /// </summary>
        public void InvokeCallback(string text)
        {
            _target.InvokeOperation<object>(OpInvokeCallback, w => w.WriteString(text), null, null);
        }

        /// <summary>
        /// Gets the name.
        /// </summary>
        public string GetName()
        {
            return _target.InvokeOperation(OpGetName, null, (r, _) => r.ReadString(), null);
        }

        /// <summary>
        /// Gets the name.
        /// </summary>
        public string GetObjectName(IPluginTarget obj)
        {
            return _target.InvokeOperation(OpGetObjectName, null, (r, _) => r.ReadString(), obj);
        }

        /// <summary>
        /// Gets the child plugin.
        /// </summary>
        public TestPlugin GetChild(string name)
        {
            return _target.InvokeOperation(OpGetChild, w => w.WriteString(name), (_, t) => new TestPlugin(t), null);
        }

        /// <summary>
        /// Gets the node id.
        /// </summary>
        public Guid GetNodeId()
        {
            return _target.InvokeOperation(OpGetNodeId, null, (r, _) => r.ReadGuid(), null) ?? Guid.Empty;
        }

        /// <summary>
        /// Gets the callback response that is saved in Java object.
        /// </summary>
        public string GetCallbackResponse()
        {
            return _target.InvokeOperation(OpGetCallbackResponse, null, (r, _) => r.ReadString(), null);
        }

        /// <summary>
        /// Gets the target.
        /// </summary>
        public IPluginTarget Target
        {
            get { return _target; }
        }

        /// <summary>
        /// Performs application-defined tasks associated with freeing, releasing, or resetting unmanaged resources.
        /// </summary>
        public void Dispose()
        {
            _target.Dispose();
        }
    }

    /// <summary>
    /// Expected Ignite plugin entry point is IIgnite extension method,
    /// so users simply say ignite.GetTestPlugin().
    /// </summary>
    public static class TestPluginExtensions
    {
        /** Plugin name. */
        private const string PluginName = "PlatformTestPlugin";

        /// <summary>
        /// Gets the test plugin.
        /// </summary>
        public static TestPlugin GetTestPlugin(this IIgnite ignite)
        {
            var target = ignite.GetPluginTarget(PluginName);

            return new TestPlugin(target);
        }
    }
}
