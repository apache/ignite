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

namespace Apache.Ignite.Benchmarks.Model
{
    using Apache.Ignite.Core.Binary;

    public class Doubles : IBinarizable
    {
        public double[] Data = new double[1000 * 32];

        public Doubles()
        {
            // No-op
        }

        public Doubles Init(Random random)
        {
            for (var i = 0; i < Data.Length; i++)
            {
                Data[i] = random.NextDouble();
            }
            return this;
        }

        public static Doubles[] GetDoubles(int size)
        {
            var data = new Doubles[size];
            var random = new Random();
            for (var i = 0; i < data.Length; i++)
            {
                data[i] = new Doubles().Init(random);
            }
            return data;
        }

        public void WriteBinary(IBinaryWriter writer)
        {
            writer.WriteDoubleArray("data", Data);
        }

        public void ReadBinary(IBinaryReader reader)
        {
            Data = reader.ReadDoubleArray("data");
        }
    }
}
