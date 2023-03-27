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

// ReSharper disable EqualExpressionComparison
namespace Apache.Ignite.Core.Tests.Client
{
    using Apache.Ignite.Core.Impl.Client;
    using NUnit.Framework;

    /// <summary>
    /// Tests for <see cref="ClientProtocolVersion"/>.
    /// </summary>
    public class ClientProtocolVersionTest
    {
        /// <summary>
        /// Tests constructors.
        /// </summary>
        [Test]
        public void TestConstructor()
        {
            var v0 = new ClientProtocolVersion();
            Assert.AreEqual(0, v0.Major);
            Assert.AreEqual(0, v0.Minor);
            Assert.AreEqual(0, v0.Maintenance);
            
            var v1 = new ClientProtocolVersion(2, 4, 8);
            Assert.AreEqual(2, v1.Major);
            Assert.AreEqual(4, v1.Minor);
            Assert.AreEqual(8, v1.Maintenance);
        }
        
        /// <summary>
        /// Tests comparison for equality.
        /// </summary>
        [Test]
        public void TestEqualityComparison()
        {
            Assert.AreEqual(
                new ClientProtocolVersion(), 
                new ClientProtocolVersion());
            
            Assert.IsTrue(new ClientProtocolVersion() == new ClientProtocolVersion());
            
            Assert.AreEqual(
                new ClientProtocolVersion(1, 2, 3), 
                new ClientProtocolVersion(1, 2, 3));
            
            Assert.IsTrue(
                new ClientProtocolVersion(1, 2, 3) ==  
                new ClientProtocolVersion(1, 2, 3));

            Assert.IsTrue(
                new ClientProtocolVersion(1, 2, 3).Equals(
                    new ClientProtocolVersion(1, 2, 3)));

            Assert.IsTrue(
                new ClientProtocolVersion(1, 2, 3).Equals(
                    (object) new ClientProtocolVersion(1, 2, 3)));
            
            Assert.IsFalse(
                new ClientProtocolVersion(1, 2, 3).Equals(null));
            
            Assert.IsFalse(
                new ClientProtocolVersion(1, 2, 3) !=  
                new ClientProtocolVersion(1, 2, 3));
            
            Assert.IsFalse(
                new ClientProtocolVersion(1, 2, 3) ==  
                new ClientProtocolVersion(1, 2, 4));
            
            Assert.AreNotEqual(
                new ClientProtocolVersion(1, 2, 3), 
                new ClientProtocolVersion(1, 2, 4));
            
            Assert.AreNotEqual(
                new ClientProtocolVersion(1, 2, 3), 
                new ClientProtocolVersion(1, 3, 3));
            
            Assert.AreNotEqual(
                new ClientProtocolVersion(1, 2, 3), 
                new ClientProtocolVersion(0, 2, 3));
        }

        /// <summary>
        /// Test relational comparison.
        /// </summary>
        [Test]
        public void TestRelationalComparison()
        {
            Assert.AreEqual(0, new ClientProtocolVersion(1, 2, 3)
                .CompareTo(new ClientProtocolVersion(1, 2, 3)));
            
            Assert.AreEqual(1, new ClientProtocolVersion(1, 2, 3)
                .CompareTo(new ClientProtocolVersion(1, 1, 1)));
            
            Assert.AreEqual(1, new ClientProtocolVersion(1, 2, 3)
                .CompareTo(new ClientProtocolVersion(0, 100, 200)));
            
            Assert.AreEqual(-1, new ClientProtocolVersion(1, 2, 3)
                .CompareTo(new ClientProtocolVersion(2, 0, 0)));
            
            Assert.AreEqual(-1, new ClientProtocolVersion(1, 2, 3)
                .CompareTo(new ClientProtocolVersion(1, 2, 4)));
        }

        /// <summary>
        /// Test relational comparison operators.
        /// </summary>
        [Test]
        public void TestRelationalComparisonOperators()
        {
            var v1 = new ClientProtocolVersion(1, 1, 1);
            var v2 = new ClientProtocolVersion(1, 1, 2);
            
            Assert.IsTrue(v1 < v2);
            Assert.IsTrue(v1 <= v2);
            Assert.IsTrue(v2 > v1);
            Assert.IsTrue(v2 >= v1);
        }

        /// <summary>
        /// Tests GetHashCode method.
        /// </summary>
        [Test]
        public void TestGetHashCode()
        {
            Assert.AreEqual(
                new ClientProtocolVersion(1, 2, 3).GetHashCode(), 
                new ClientProtocolVersion(1, 2, 3).GetHashCode());

            Assert.AreNotEqual(
                new ClientProtocolVersion(1, 2, 3).GetHashCode(), 
                new ClientProtocolVersion(1, 2, 5).GetHashCode());
        }

        /// <summary>
        /// Tests ToString method.
        /// </summary>
        [Test]
        public void TestToString()
        {
            Assert.AreEqual(
                "16.42.128", 
                new ClientProtocolVersion(16, 42, 128).ToString());
        }
    }
}