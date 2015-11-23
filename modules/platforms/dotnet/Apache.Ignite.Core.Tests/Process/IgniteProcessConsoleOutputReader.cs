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

namespace Apache.Ignite.Core.Tests.Process
{
    using System;
    using System.Diagnostics;

    /// <summary>
    /// Output reader pushing data to the console.
    /// </summary>
    public class IgniteProcessConsoleOutputReader : IIgniteProcessOutputReader
    {
        /** Out message format. */
        private static readonly string OutFormat = ">>> {0} OUT: {1}";

        /** Error message format. */
        private static readonly string ErrFormat = ">>> {0} ERR: {1}";

        /** <inheritDoc /> */
        public void OnOutput(Process proc, string data, bool err)
        {
            Console.WriteLine(err ? ErrFormat : OutFormat, proc.Id, data);
        }
    }
}
