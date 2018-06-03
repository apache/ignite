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

namespace Apache.Ignite.ExamplesDll.Datagrid
{
    using Apache.Ignite.Core.Cache.Event;

    /// <summary>
    /// Filter for continuous query example.
    /// </summary>
    public class ContinuousQueryFilter : ICacheEntryEventFilter<int, string>
    {
        /// <summary> Threshold. </summary>
        private readonly int _threshold;

        /// <summary>
        /// Constructor.
        /// </summary>
        /// <param name="threshold">Threshold.</param>
        public ContinuousQueryFilter(int threshold)
        {
            _threshold = threshold;
        }

        /// <summary>
        /// Evaluates cache entry event.
        /// </summary>
        /// <param name="evt">Event.</param>
        public bool Evaluate(ICacheEntryEvent<int, string> evt)
        {
            return evt.Key >= _threshold;
        }
    }
}
