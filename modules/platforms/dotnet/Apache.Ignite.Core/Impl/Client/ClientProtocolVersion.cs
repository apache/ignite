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

namespace Apache.Ignite.Core.Impl.Client
{
    using System;

    /// <summary>
    /// Client protocol version.
    /// </summary>
    internal struct ClientProtocolVersion : IEquatable<ClientProtocolVersion>, IComparable<ClientProtocolVersion>
    {
        /** */
        private readonly short _major;

        /** */
        private readonly short _minor;

        /** */
        private readonly short _maintenance;

        /// <summary>
        /// Initializes a new instance of the <see cref="ClientProtocolVersion"/> struct.
        /// </summary>
        public ClientProtocolVersion(short major, short minor, short maintenance)
        {
            _major = major;
            _minor = minor;
            _maintenance = maintenance;
        }

        /// <summary>
        /// Gets the major part.
        /// </summary>
        public short Major
        {
            get { return _major; }
        }

        /// <summary>
        /// Gets the minor part.
        /// </summary>
        public short Minor
        {
            get { return _minor; }
        }

        /// <summary>
        /// Gets the maintenance part.
        /// </summary>
        public short Maintenance
        {
            get { return _maintenance; }
        }

        /// <summary>
        /// Compare this version to other version.
        /// </summary>
        /// <param name="other"></param>
        /// <returns></returns>
        public int CompareTo(ClientProtocolVersion other)
        {
            int res = Major - other.Major;

            if (res == 0)
            {
                res = Minor - other.Minor;

                if (res == 0)
                    res = Maintenance - other.Maintenance;
            }

            return res;
        }

        /// <summary>
        /// Returns a value indicating whether specified instance equals to current.
        /// </summary>
        public bool Equals(ClientProtocolVersion other)
        {
            return _major == other._major && _minor == other._minor && _maintenance == other._maintenance;
        }

        /** <inheritdoc /> */
        public override bool Equals(object obj)
        {
            if (ReferenceEquals(null, obj))
            {
                return false;
            }

            return obj is ClientProtocolVersion && Equals((ClientProtocolVersion) obj);
        }

        /// <summary>
        /// Equality operator.
        /// </summary>
        public static bool operator ==(ClientProtocolVersion left, ClientProtocolVersion right)
        {
            return left.Equals(right);
        }

        /// <summary>
        /// Inequality operator.
        /// </summary>
        public static bool operator !=(ClientProtocolVersion left, ClientProtocolVersion right)
        {
            return !left.Equals(right);
        }

        /// <summary>
        /// Less-than operator.
        /// </summary>
        public static bool operator <(ClientProtocolVersion left, ClientProtocolVersion right)
        {
            return left.CompareTo(right) < 0;
        }

        /// <summary>
        /// Less-or-equal-than operator.
        /// </summary>
        public static bool operator <=(ClientProtocolVersion left, ClientProtocolVersion right)
        {
            return left.CompareTo(right) <= 0;
        }

        /// <summary>
        /// Greater-than operator.
        /// </summary>
        public static bool operator >(ClientProtocolVersion left, ClientProtocolVersion right)
        {
            return left.CompareTo(right) > 0;
        }

        /// <summary>
        /// Greater-or-equal-than operator.
        /// </summary>
        public static bool operator >=(ClientProtocolVersion left, ClientProtocolVersion right)
        {
            return left.CompareTo(right) >= 0;
        }

        /** <inheritdoc /> */
        public override int GetHashCode()
        {
            unchecked
            {
                var hashCode = _major.GetHashCode();
                hashCode = (hashCode * 397) ^ _minor.GetHashCode();
                hashCode = (hashCode * 397) ^ _maintenance.GetHashCode();
                return hashCode;
            }
        }

        /** <inheritdoc /> */
        public override string ToString()
        {
            return string.Format("{0}.{1}.{2}", Major, Minor, Maintenance);
        }
    }
}
