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

namespace Apache.Ignite.BenchmarkDotNet.Binary
{
    using global::BenchmarkDotNet.Attributes;

    public class DecimalScaleBenchmark
    {
        private const decimal Value = 123.456m;

        private const int Scale = 21;

        [Benchmark]
        public decimal MathPow()
        {
            return Value * (decimal)System.Math.Pow(10, Scale);
        }

        [Benchmark]
        public decimal Loop()
        {
            decimal result = Value;

            for (var i = 0; i < Scale; i++)
            {
                result *= 10;
            }

            return result;
        }
    }
}
