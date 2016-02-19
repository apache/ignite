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

namespace Apache.Ignite.Core.Tests.Compute
{
    using System;
    using System.Diagnostics;
    using NUnit.Framework;

    /// <summary>
    /// Tests compute in a cluster with Java-only and .NET nodes.
    /// </summary>
    public class MixedClusterTest
    {
        /// <summary>
        /// Tests the compute.
        /// </summary>
        [Test]
        public void TestCompute()
        {
            var proc = StartJavaNode();

            try
            {
                using (var ignite = Ignition.Start())
                {
                    
                }

            }
            finally 
            {
                proc.Kill();
            }
        }

        /// <summary>
        /// Starts the java node.
        /// </summary>
        private static Process StartJavaNode()
        {
            // TODO
            return Process.Start("");
        }
    }
}
