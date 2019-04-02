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
    using System.Reflection;
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
        private static readonly byte[] ByteArray = {24, 229, 167, 236, 158, 50, 2, 18, 106, 105, 188, 35, 26, 107, 150, 91, 193, 215, 61, 238};

        private IgniteProductVersion _defaultVersion;

        [SetUp]
        public void SetUp()
        {
            var timeStamp = (new DateTime(2018, 1, 1).Ticks - BinaryUtils.JavaDateTicks) / 1000;
            _defaultVersion = new IgniteProductVersion(2, 5, 7, timeStamp, null);
        }

        [Test]
        public void TestBinaryConstructor()
        {
            IBinaryRawReader reader = SetUpRawBinaryReader(_defaultVersion.RevisionHash);

            IgniteProductVersion deserializedVersion = new IgniteProductVersion(reader);

            Assert.AreEqual(_defaultVersion, deserializedVersion);
        }

        private IBinaryRawReader SetUpRawBinaryReader(byte[] hash)
        {
            var marsh = BinaryUtils.Marshaller;

            using (var stream = new BinaryHeapStream(128))
            {
                var writer = marsh.StartMarshal(stream);

                writer.WriteByte(_defaultVersion.Major);
                writer.WriteByte(_defaultVersion.Minor);
                writer.WriteByte(_defaultVersion.Maintenance);
                writer.WriteLong(_defaultVersion.RevisionTimestamp);
                writer.WriteByteArray(hash);

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
            Assert.AreEqual("2.5.7#20180101-sha1:", _defaultVersion.ToString());
        }

        [Test]
        public void TestToStringWithLimitedHashValue()
        {
            var hash = new byte[] {24, 229};
            IBinaryRawReader reader = SetUpRawBinaryReader(hash);

            IgniteProductVersion deserializedVersion = new IgniteProductVersion(reader);
            Assert.AreEqual("2.5.7#20180101-sha1:18", deserializedVersion.ToString());
        }

        [Test]
        public void TestLocalNodeGetVersion()
        {
            using (IIgnite ignite = Ignition.Start(TestUtils.GetTestConfiguration()))
            {
                IgniteProductVersion nodeVersion = ignite.GetCluster().GetLocalNode().Version;

                Assert.GreaterOrEqual(nodeVersion.Major, 2);
                Assert.GreaterOrEqual(nodeVersion.Minor, 0);
                Assert.GreaterOrEqual(nodeVersion.Maintenance, 0);
            }
        }

        [Test]
        public void TestIgniteGetVersionAndNodeVersionAreEqual()
        {
            using (IIgnite ignite = Ignition.Start(TestUtils.GetTestConfiguration()))
            {
                IgniteProductVersion version = ignite.GetVersion();
                IgniteProductVersion nodeVersion = ignite.GetCluster().GetLocalNode().Version;

                Assert.AreEqual(version, nodeVersion);
            }
        }

        [Test]
        public void TestClientVersionIsMoreOrEqualsServerNodeVersion()
        {
            using (IIgnite ignite = Ignition.Start(TestUtils.GetTestConfiguration()))
            {
                IgniteProductVersion version = ignite.GetVersion();

                Version clientVer = Assembly.GetExecutingAssembly().GetName().Version;
                var expectedVersion = new IgniteProductVersion((byte) clientVer.Major, (byte) clientVer.Minor,
                    (byte) clientVer.Build, 0, null);

                Assert.IsTrue(expectedVersion.CompareTo(version) >= 0);
            }
        }

        [Test]
        public void TestUnsignedStringRepresentation()
        {
            var version = new IgniteProductVersion(1, 2, 3, "rc1", 4, ByteArray);
            Assert.AreEqual("1.2.3#19700101-sha1:18e5a7ec", version.ToString());
        }

        [Test]
        public void TestSignedByteArrayIsTheSameForStringRepresentation()
        {
            sbyte[] signed = Array.ConvertAll(ByteArray, b => unchecked((sbyte) b));

            // ReSharper disable once PossibleInvalidCastException
            var unsignedCast = (byte[]) (Array) signed;

            var versionSigned = new IgniteProductVersion(1, 2, 3, "rc1", 4, unsignedCast);
            var versionUnsigned = new IgniteProductVersion(1, 2, 3, "rc1", 4, ByteArray);

            Assert.AreEqual(versionSigned.ToString(), versionUnsigned.ToString());
        }
    }
}
