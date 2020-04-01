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

namespace Apache.Ignite.Core.Tests.Cache.Near
{
    using System;
    using Apache.Ignite.Core.Cache.Configuration;
    using NUnit.Framework;

    /// <summary>
    /// Test class.
    /// </summary>
    public class Foo : IEquatable<Foo>
    {
        public Foo(int bar = 0)
        {
            Bar = bar;
        }

        [QuerySqlField]
        public readonly int Bar;

        [QuerySqlField]
        public readonly string TestName = TestContext.CurrentContext.Test.Name;

        public override string ToString()
        {
            return String.Format("Foo [Bar={0}, TestName={1}]", Bar, TestName);
        }

        public bool Equals(Foo other)
        {
            return other != null && Bar == other.Bar && TestName == other.TestName;
        }

        public override bool Equals(object obj)
        {
            if (ReferenceEquals(null, obj)) return false;
            if (ReferenceEquals(this, obj)) return true;
            if (obj.GetType() != this.GetType()) return false;
            return Equals((Foo) obj);
        }

        public override int GetHashCode()
        {
            unchecked
            {
                return (Bar * 397) ^ (TestName != null ? TestName.GetHashCode() : 0);
            }
        }
    }
}