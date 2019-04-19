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
 
namespace Apache.Ignite.Benchmarks.Model
{
    using System;
    using Apache.Ignite.Core.Binary;

    /// <summary>
    /// Model with a single double[] field.
    /// </summary>
    public class Doubles : IBinarizable
    {
        /** */
        private double[] _data = new double[1000 * 32];

        /// <summary>
        /// Initializes the instance.
        /// </summary>
        private Doubles Init(Random random)
        {
            for (var i = 0; i < _data.Length; i++)
            {
                _data[i] = random.NextDouble();
            }
            return this;
        }

        /// <summary>
        /// Gets the instances.
        /// </summary>
        public static Doubles[] GetInstances(int size)
        {
            var data = new Doubles[size];
            var random = new Random();
            for (var i = 0; i < data.Length; i++)
            {
                data[i] = new Doubles().Init(random);
            }
            return data;
        }

        /** <inheritdoc /> */
        public void WriteBinary(IBinaryWriter writer)
        {
            writer.WriteDoubleArray("data", _data);
        }

        /** <inheritdoc /> */
        public void ReadBinary(IBinaryReader reader)
        {
            _data = reader.ReadDoubleArray("data");
        }
    }
}
