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

namespace Apache.Ignite.Core.Tests.Client.Cache
{
    using Apache.Ignite.Core.Cache.Affinity;

    public sealed class TestKeyWithUserObjectAffinity
    {
        [AffinityKeyMapped]
        private readonly TestKey _key;

        private readonly string _s;

        public TestKeyWithUserObjectAffinity(TestKey key, string s)
        {
            _key = key;
            _s = s;
        }

        private bool Equals(TestKeyWithUserObjectAffinity other)
        {
            return _key.Equals(other._key) && string.Equals(_s, other._s);
        }

        public override bool Equals(object obj)
        {
            if (ReferenceEquals(null, obj)) return false;
            if (ReferenceEquals(this, obj)) return true;
            if (obj.GetType() != GetType()) return false;
            return Equals((TestKeyWithUserObjectAffinity) obj);
        }

        public override int GetHashCode()
        {
            unchecked
            {
                return (_key.GetHashCode() * 397) ^ (_s != null ? _s.GetHashCode() : 0);
            }
        }
    }
}
