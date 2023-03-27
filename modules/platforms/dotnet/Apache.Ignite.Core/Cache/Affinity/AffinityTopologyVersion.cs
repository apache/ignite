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

namespace Apache.Ignite.Core.Cache.Affinity
{
    using System;
    using Apache.Ignite.Core.Cluster;

    /// <summary>
    /// Affinity topology version.
    /// </summary>
    public struct AffinityTopologyVersion : IEquatable<AffinityTopologyVersion>, IComparable<AffinityTopologyVersion>
    {
        /** */
        private readonly long _version;

        /** */
        private readonly int _minorVersion;

        /// <summary>
        /// Initializes a new instance of the <see cref="AffinityTopologyVersion"/> struct.
        /// </summary>
        /// <param name="version">The version.</param>
        /// <param name="minorVersion">The minor version.</param>
        public AffinityTopologyVersion(long version, int minorVersion)
        {
            _version = version;
            _minorVersion = minorVersion;
        }

        /// <summary>
        /// Gets the major version, same as <see cref="ICluster.TopologyVersion"/>.
        /// </summary>
        public long Version
        {
            get { return _version; }
        }

        /// <summary>
        /// Gets the minor version, which is increased when new caches start.
        /// </summary>
        public int MinorVersion
        {
            get { return _minorVersion; }
        }

        /// <summary>
        /// Indicates whether the current object is equal to another object of the same type.
        /// </summary>
        /// <param name="other">An object to compare with this object.</param>
        /// <returns>
        /// true if the current object is equal to the <paramref name="other" /> parameter; otherwise, false.
        /// </returns>
        public bool Equals(AffinityTopologyVersion other)
        {
            return _version == other._version && _minorVersion == other._minorVersion;
        }

        /// <summary>
        /// Determines whether the specified <see cref="System.Object" />, is equal to this instance.
        /// </summary>
        /// <param name="obj">The <see cref="System.Object" /> to compare with this instance.</param>
        /// <returns>
        /// <c>true</c> if the specified <see cref="System.Object" /> is equal to this instance; otherwise, 
        /// <c>false</c>.
        /// </returns>
        public override bool Equals(object obj)
        {
            if (ReferenceEquals(null, obj)) return false;
            return obj is AffinityTopologyVersion && Equals((AffinityTopologyVersion) obj);
        }

        /// <summary>
        /// Returns a hash code for this instance.
        /// </summary>
        /// <returns>
        /// A hash code for this instance, suitable for use in hashing algorithms and data structures like a hash table. 
        /// </returns>
        public override int GetHashCode()
        {
            unchecked
            {
                return (_version.GetHashCode()*397) ^ _minorVersion;
            }
        }

        /// <summary>
        /// Implements the operator ==.
        /// </summary>
        /// <param name="left">The left operand.</param>
        /// <param name="right">The right operand.</param>
        /// <returns>
        /// The result of the operator.
        /// </returns>
        public static bool operator ==(AffinityTopologyVersion left, AffinityTopologyVersion right)
        {
            return left.Equals(right);
        }

        /// <summary>
        /// Implements the operator !=.
        /// </summary>
        /// <param name="left">The left operand.</param>
        /// <param name="right">The right operand.</param>
        /// <returns>
        /// The result of the operator.
        /// </returns>
        public static bool operator !=(AffinityTopologyVersion left, AffinityTopologyVersion right)
        {
            return !left.Equals(right);
        }

        /// <summary>
        /// Implements the operator 'less than'.
        /// </summary>
        /// <param name="left">The left operand.</param>
        /// <param name="right">The right operand.</param>
        /// <returns>
        /// The result of the operator.
        /// </returns>
        public static bool operator <(AffinityTopologyVersion left, AffinityTopologyVersion right)
        {
            return left.CompareTo(right) < 0;
        }

        /// <summary>
        /// Implements the operator 'greater than'.
        /// </summary>
        /// <param name="left">The left operand.</param>
        /// <param name="right">The right operand.</param>
        /// <returns>
        /// The result of the operator.
        /// </returns>
        public static bool operator >(AffinityTopologyVersion left, AffinityTopologyVersion right)
        {
            return left.CompareTo(right) > 0;
        }

        /// <summary>
        /// Implements the operator 'less or equal than'.
        /// </summary>
        /// <param name="left">The left operand.</param>
        /// <param name="right">The right operand.</param>
        /// <returns>
        /// The result of the operator.
        /// </returns>
        public static bool operator <=(AffinityTopologyVersion left, AffinityTopologyVersion right)
        {
            return left.CompareTo(right) <= 0;
        }

        /// <summary>
        /// Implements the operator 'greater or equal than'.
        /// </summary>
        /// <param name="left">The left operand.</param>
        /// <param name="right">The right operand.</param>
        /// <returns>
        /// The result of the operator.
        /// </returns>
        public static bool operator >=(AffinityTopologyVersion left, AffinityTopologyVersion right)
        {
            return left.CompareTo(right) >= 0;
        }

        /// <summary>
        /// Returns a <see cref="string" /> that represents this instance.
        /// </summary>
        /// <returns>
        /// A <see cref="string" /> that represents this instance.
        /// </returns>
        public override string ToString()
        {
            return string.Format("AffinityTopologyVersion [Version={0}, MinorVersion={1}]", _version, _minorVersion);
        }

        /// <summary>
        /// Compares the current instance with another object of the same type and returns an integer that indicates
        /// whether the current instance precedes, follows,
        /// or occurs in the same position in the sort order as the other object.
        /// </summary>
        public int CompareTo(AffinityTopologyVersion other)
        {
            var versionComparison = _version.CompareTo(other._version);

            return versionComparison != 0
                ? versionComparison
                : _minorVersion.CompareTo(other._minorVersion);
        }
    }
}
