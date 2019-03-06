/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
 */

namespace Apache.Ignite.Benchmarks.Binary
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using Apache.Ignite.Benchmarks.Model;
    using Apache.Ignite.Core.Binary;
    using Apache.Ignite.Core.Impl.Binary;
    using Apache.Ignite.Core.Impl.Memory;

    /// <summary>
    /// Binary read benchmark.
    /// </summary>
    internal class BinarizableReadBenchmark : BenchmarkBase
    {
        /** Marshaller. */
        private readonly Marshaller _marsh;

        /** Memory manager. */
        private readonly PlatformMemoryManager _memMgr = new PlatformMemoryManager(1024);

        /** Memory chunk. */
        private readonly IPlatformMemory _mem;

        /** Pre-allocated address. */
        private readonly Address _address = BenchmarkUtils.GetRandomAddress();

        /** Pre-allocated model. */
        private readonly TestModel _model = new TestModel
        {
            Byte = 5,
            Boolean = true,
            BooleanArray = new[] {true, false, false, false, true, true},
            ByteArray = new byte[] {128, 1, 2, 3, 5, 6, 8, 9, 14},
            Char = 'h',
            CharArray = new[] {'b', 'n', 'm', 'q', 'w', 'e', 'r', 't', 'y'},
            Date = DateTime.Now,
            DateArray = Enumerable.Range(1, 15).Select(x => (DateTime?) DateTime.Now.AddDays(x)).ToArray(),
            Decimal = decimal.MinValue,
            DecimalArray = new decimal?[] {1.1M, decimal.MinValue, decimal.MaxValue, decimal.MinusOne, decimal.One},
            Double = double.MaxValue/2,
            DoubleArray = new[] {double.MaxValue, double.MinValue, double.Epsilon, double.NegativeInfinity},
            Float = 98,
            FloatArray = new[] {float.MinValue, float.MaxValue, 10F, 36F},
            Guid = Guid.NewGuid(),
            GuidArray = Enumerable.Range(1, 9).Select(x => (Guid?) Guid.NewGuid()).ToArray(),
            Int = -90,
            IntArray = new[] {128, 1, 2, 3, 5, 6, 8, 9, 14},
            Long = long.MinValue,
            LongArray = Enumerable.Range(1, 12).Select(x => (long) x).ToArray(),
            Short = 67,
            ShortArray = Enumerable.Range(100, 12).Select(x => (short) x).ToArray(),
            String = "String value test 123",
            StringArray = Enumerable.Range(1, 13).Select(x => Guid.NewGuid().ToString()).ToArray()
        };

        /// <summary>
        /// Initializes a new instance of the <see cref="BinarizableReadBenchmark"/> class.
        /// </summary>
        public BinarizableReadBenchmark()
        {
            _marsh = new Marshaller(new BinaryConfiguration
            {
                TypeConfigurations = new List<BinaryTypeConfiguration>
                {
                    new BinaryTypeConfiguration(typeof (Address)),
                    new BinaryTypeConfiguration(typeof (TestModel))
                }
            });

            _mem = _memMgr.Allocate();

            var stream = _mem.GetStream();

            //_marsh.StartMarshal(stream).Write(_model);
            _marsh.StartMarshal(stream).Write(_address);

            stream.SynchronizeOutput();
        }

        /// <summary>
        /// Populate descriptors.
        /// </summary>
        /// <param name="descs">Descriptors.</param>
        protected override void GetDescriptors(ICollection<BenchmarkOperationDescriptor> descs)
        {
            descs.Add(BenchmarkOperationDescriptor.Create("ReadTestModel", ReadTestModel, 1));
        }

        /// <summary>
        /// Write address.
        /// </summary>
        /// <param name="state">State.</param>
        private void ReadTestModel(BenchmarkState state)
        {
            //var model = _marsh.StartUnmarshal(_mem.GetStream()).ReadObject<TestModel>();

            //if (model.Byte != _model.Byte)
            //    throw new InvalidOperationException();

            var model = _marsh.StartUnmarshal(_mem.GetStream()).ReadObject<Address>();

            if (model.FlatNumber != _address.FlatNumber)
                throw new InvalidOperationException();

        }
    }
}
