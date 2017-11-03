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

namespace Apache.Ignite.Core.Impl.Common
{
    /// <summary>
    /// Fowler-Noll-Vo hash function.
    /// </summary>
    internal static class Fnv1Hash
    {
        /** Basis. */
        public const int Basis = unchecked((int) 0x811C9DC5);

        /** Prime. */
        public const int Prime = 0x01000193;

        /// <summary>
        /// Updates the hashcode with next int.
        /// </summary>
        /// <param name="current">The current.</param>
        /// <param name="next">The next.</param>
        /// <returns>Updated hashcode.</returns>
        public static int Update(int current, int next)
        {
            unchecked
            {
                current ^= next & 0xFF;
                current *= Prime;

                current ^= (next >> 8) & 0xFF;
                current *= Prime;

                current ^= (next >> 16) & 0xFF;
                current *= Prime;

                current ^= (next >> 24) & 0xFF;
                current *= Prime;

                return current;
            }
        }
    }
}
