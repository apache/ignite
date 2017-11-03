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

namespace Apache.Ignite.Core.Tests
{
    using System;
    using System.Linq;
    using System.Configuration;
    using System.Reflection;

    /// <summary>
    /// Replaces app.config at runtime for test purposes.
    /// </summary>
    public static class TestAppConfig
    {
        /// <summary>
        /// Changes the current app config with the specified one.
        /// </summary>
        /// <param name="path">The path.</param>
        /// <returns>Disposable object that will revert the change when disposed.</returns>
        public static IDisposable Change(string path)
        {
            return new ChangeAppConfig(path);
        }

        /// <summary>
        /// Disposable config changer.
        /// </summary>
        private class ChangeAppConfig : IDisposable
        {
            /** */
            private readonly string _oldConfig =
                AppDomain.CurrentDomain.GetData("APP_CONFIG_FILE").ToString();

            /** */
            private bool _isDisposed;

            /// <summary>
            /// Initializes a new instance of the <see cref="ChangeAppConfig"/> class.
            /// </summary>
            /// <param name="path">The path.</param>
            public ChangeAppConfig(string path)
            {
                AppDomain.CurrentDomain.SetData("APP_CONFIG_FILE", path);
                ResetConfigMechanism();
            }

            /** <inheritdoc /> */
            public void Dispose()
            {
                if (!_isDisposed)
                {
                    AppDomain.CurrentDomain.SetData("APP_CONFIG_FILE", _oldConfig);
                    ResetConfigMechanism();


                    _isDisposed = true;
                }
                GC.SuppressFinalize(this);
            }

            /// <summary>
            /// Resets the internal configuration mechanism.
            /// </summary>
            private static void ResetConfigMechanism()
            {
                // ReSharper disable PossibleNullReferenceException

                typeof(ConfigurationManager)
                    .GetField("s_initState", BindingFlags.NonPublic | BindingFlags.Static)
                    .SetValue(null, 0);

                typeof(ConfigurationManager)
                    .GetField("s_configSystem", BindingFlags.NonPublic | BindingFlags.Static)
                    .SetValue(null, null);

                typeof(ConfigurationManager)
                    .Assembly.GetTypes().First(x => x.FullName == "System.Configuration.ClientConfigPaths")
                    .GetField("s_current", BindingFlags.NonPublic | BindingFlags.Static)
                    .SetValue(null, null);
                
                // ReSharper restore PossibleNullReferenceException
            }
        }
    }
}
