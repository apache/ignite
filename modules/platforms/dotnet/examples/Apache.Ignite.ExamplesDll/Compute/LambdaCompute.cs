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

using System;
using System.Linq;

namespace Apache.Ignite.ExamplesDll.Compute
{
    using Apache.Ignite.Core.Compute;

    /// <summary>
    /// Runs lambda-based Compute jobs.
    /// </summary>
    public static class LambdaCompute
    {
        /// <summary>
        /// Counts the characters in a string using lambda-based compute task.
        /// </summary>
        /// <param name="compute">The compute.</param>
        /// <param name="data">The data to count characters.</param>
        /// <returns>Character count.</returns>
        public static int CountCharacters(ICompute compute, string data)
        {
            // Split the string by spaces to count letters in each word in parallel.
            var words = data.Split().ToList();

            return compute.Apply(word =>
            {
                var len = word.Length;

                Console.WriteLine("Character count in word \"{0}\": {1}", word, len);

                return len;
            }, words, ints => ints.Sum());
        }

        /// <summary>
        /// Broadcasts the message to console on all nodes.
        /// </summary>
        /// <param name="compute">The compute.</param>
        /// <param name="message">The message.</param>
        public static void BroadcastConsoleMessage(ICompute compute, string message)
        {
            compute.Broadcast(() => Console.WriteLine(message));
        }
    }
}
