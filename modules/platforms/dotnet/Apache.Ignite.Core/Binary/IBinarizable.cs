/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 * 
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

namespace Apache.Ignite.Core.Binary
{
    /// <summary>
    /// Interface to implement custom serialization logic.
    /// </summary>
    public interface IBinarizable 
    {
        /// <summary>
        /// Writes this object to the given writer.
        /// </summary> 
        /// <param name="writer">Writer.</param>
        /// <exception cref="System.IO.IOException">If write failed.</exception>
        void WriteBinary(IBinaryWriter writer);

        /// <summary>
        /// Reads this object from the given reader.
        /// </summary> 
        /// <param name="reader">Reader.</param>
        /// <exception cref="System.IO.IOException">If read failed.</exception>
        void ReadBinary(IBinaryReader reader);
    }
}
