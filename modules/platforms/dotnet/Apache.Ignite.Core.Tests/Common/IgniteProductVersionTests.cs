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
    public class IgniteProductVersionTests : TestBase
    {
        private static readonly byte[] ByteArray = {24, 229, 167, 236, 158, 50, 2, 18, 106, 105, 188, 35, 26, 107, 150, 91, 193, 215, 61, 238};
        private static readonly DateTime DefaultReleaseDate = new DateTime(2018, 1, 1);

        private IgniteProductVersion _defaultVersion;

        [SetUp]
        public void SetUp()
        {
            _defaultVersion = new IgniteProductVersion(2, 5, 7, "Stage", DefaultReleaseDate, null);
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
                writer.WriteString(_defaultVersion.Stage);
                writer.WriteLong(BinaryUtils.DateTimeToJavaTicks(_defaultVersion.ReleaseDate));
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
            IgniteProductVersion nodeVersion = Ignite.GetCluster().GetLocalNode().Version;

            Assert.GreaterOrEqual(nodeVersion.Major, 2);
            Assert.GreaterOrEqual(nodeVersion.Minor, 0);
            Assert.GreaterOrEqual(nodeVersion.Maintenance, 0);
            Assert.GreaterOrEqual(nodeVersion.Stage, "SNAPSHOT");
        }

        [Test]
        public void TestIgniteGetVersionAndNodeVersionAreEqual()
        {
            IgniteProductVersion version = Ignite.GetVersion();
            IgniteProductVersion nodeVersion = Ignite.GetCluster().GetLocalNode().Version;

            Assert.AreEqual(version, nodeVersion);
        }

        [Test]
        public void TestAssemblyVersionMatchesIgniteVersion()
        {
            Version clientVer = typeof(IIgnite).Assembly.GetName().Version;

            IgniteProductVersion version = Ignite.GetVersion();
            
            Assert.AreEqual(clientVer.Major, version.Major);
            Assert.AreEqual(clientVer.Minor, version.Minor);
            Assert.AreEqual(clientVer.Build, version.Maintenance);
        }

        [Test]
        public void TestUnsignedStringRepresentation()
        {
            var version = new IgniteProductVersion(1, 2, 3, "rc1", new DateTime(2019, 1, 1), ByteArray);
            Assert.AreEqual("1.2.3#20190101-sha1:18e5a7ec", version.ToString());
        }

        [Test]
        public void TestSignedByteArrayIsTheSameForStringRepresentation()
        {
            sbyte[] signed = Array.ConvertAll(ByteArray, b => unchecked((sbyte) b));

            // ReSharper disable once PossibleInvalidCastException
            var unsignedCast = (byte[]) (Array) signed;

            var versionSigned = new IgniteProductVersion(1, 2, 3, "rc1", DefaultReleaseDate, unsignedCast);
            var versionUnsigned = new IgniteProductVersion(1, 2, 3, "rc1", DefaultReleaseDate, ByteArray);

            Assert.AreEqual(versionSigned.ToString(), versionUnsigned.ToString());
        }
    }
}
