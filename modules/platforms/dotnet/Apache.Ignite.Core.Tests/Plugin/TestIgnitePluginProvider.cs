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
    using Apache.Ignite.Core.Plugin;
    using NUnit.Framework;

    /// <summary>
    /// Test provider.
    /// </summary>
    // ReSharper disable once ClassNeverInstantiated.Global
    public class TestIgnitePluginProvider : IPluginProvider
    {
        /** */
        public const string PluginName = "TestPlugin1";

        /** */
        private readonly TestIgnitePlugin _plugin;

        /// <summary>
        /// Initializes a new instance of the <see cref="TestIgnitePluginProvider"/> class.
        /// </summary>
        public TestIgnitePluginProvider()
        {
            Name = PluginName;
            _plugin = new TestIgnitePlugin(this);
        }
        
        /** <inheritdoc /> */
        public string Name { get; set; }

        /** <inheritdoc /> */
        public string Copyright
        {
            get { return "Copyright (c) FooBar."; }
        }

        /** <inheritdoc /> */
        public T GetPlugin<T>() where T : class
        {
            var p = _plugin as T;

            if (p != null)
                return p;

            throw new Exception("Invalid plugin type: " + typeof(T));
        }

        /** <inheritdoc /> */
        public void Start(IPluginContext context)
        {
            Context = context;
        }

        /** <inheritdoc /> */
        public void Stop(bool cancel)
        {
            Stopped = cancel;
        }

        /** <inheritdoc /> */
        public void OnIgniteStart()
        {
            Started = true;

            Assert.NotNull(Context);
            Assert.NotNull(Context.Ignite);
            Assert.NotNull(Context.IgniteConfiguration);
        }

        /// <summary>
        /// Gets or sets a value indicating whether this <see cref="TestIgnitePluginProvider"/> is started.
        /// </summary>
        public bool Started { get; set; }

        /// <summary>
        /// Gets or sets a value indicating whether this <see cref="TestIgnitePluginProvider"/> is stopped.
        /// </summary>
        public bool? Stopped { get; set; }

        /// <summary>
        /// Gets the context.
        /// </summary>
        public IPluginContext Context { get; private set; }
    }
}