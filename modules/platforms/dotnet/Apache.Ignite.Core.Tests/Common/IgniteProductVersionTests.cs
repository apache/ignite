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

namespace Apache.Ignite.Core.Tests.Common
{
    using System;
    using System.IO;
    using Apache.Ignite.Core.Binary;
    using Apache.Ignite.Core.Common;
    using Apache.Ignite.Core.Impl.Binary;
    using Apache.Ignite.Core.Impl.Binary.IO;
    using NUnit.Framework;

    /// <summary>
    /// Tests the <see cref="IgniteProductVersion"/>.
    /// </summary>
    public class IgniteProductVersionTests
    {
        private IgniteProductVersion _defaultVersion;

        [SetUp]
        public void SetUp()
        {
            var timeStamp = new DateTime(2018, 1, 1).Ticks / 1000;
            _defaultVersion = new IgniteProductVersion(2, 5, 7, timeStamp, new byte[20]);
        }

        [Test]
        public void TestBinaryConstructor()
        {
            IBinaryRawReader reader = SetUpRawBinaryReader();

            IgniteProductVersion deserializedVersion = new IgniteProductVersion(reader);

            Assert.AreEqual(_defaultVersion, deserializedVersion);
        }

        private IBinaryRawReader SetUpRawBinaryReader()
        {
            var marsh = BinaryUtils.Marshaller;

            using (var stream = new BinaryHeapStream(128))
            {
                var writer = marsh.StartMarshal(stream);

                writer.WriteByte(_defaultVersion.Major);
                writer.WriteByte(_defaultVersion.Minor);
                writer.WriteByte(_defaultVersion.Maintenance);
                writer.WriteLong(_defaultVersion.RevisionTimestamp);
                writer.WriteByteArray(_defaultVersion.RevisionHash);

                stream.Seek(0, SeekOrigin.Begin);

                return marsh.StartUnmarshal(stream);
            }
        }

        [Test]
        public void TestEqualityWithNull()
        {
            Assert.IsFalse(_defaultVersion.Equals(null));
        }

        [Test]
        public void TestToString()
        {
            Assert.AreEqual("2.5.7#20180101", _defaultVersion.ToString());
        }

        [Test]
        public void TestIgniteGetVersionMethod()
        {
            using (IIgnite ignite = Ignition.Start())
            {
                var version = ignite.GetVersion();
                Assert.AreNotEqual(0, version.Major);
            }
        }
    }
}
