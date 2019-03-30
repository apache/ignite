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

namespace Apache.Ignite.Core.Common
{
    using System;
    using Apache.Ignite.Core.Binary;
    using Apache.Ignite.Core.Impl.Common;

    /// <summary>
    /// Represents node version.
    /// </summary>
    [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Design", "CA1036:OverrideMethodsOnComparableTypes")]
    public class IgniteProductVersion : IEquatable<IgniteProductVersion>, IComparable<IgniteProductVersion>
    {
        /** Major version number. */
        private readonly byte _major;

        /** Minor version number. */
        private readonly byte _minor;

        /** Maintenance version number. */
        private readonly byte _maintenance;

        /** Stage of development. */
        private readonly String _stage;

        /** Revision timestamp. */
        private readonly long _revTs;

        /** Revision hash. */
        private readonly byte[] _revHash;
        
        /// <summary>
        /// Constructor.
        /// </summary>
        /// <param name="major">Major version number.</param>
        /// <param name="minor">Minor version number.</param>
        /// <param name="maintenance">Maintenance version number.</param>
        /// <param name="revTs">Revision timestamp.</param>
        /// <param name="revHash">Revision hash.</param>
        public IgniteProductVersion(byte major, byte minor, byte maintenance, long revTs, byte[] revHash)
            : this(major, minor, maintenance, "", revTs, revHash)
        {
            // No-op.
        }

        /// <summary>
        /// Constructor.
        /// </summary>
        /// <param name="major">Major version number.</param>
        /// <param name="minor">Minor version number.</param>
        /// <param name="maintenance">Maintenance version number.</param>
        /// <param name="stage">Stage of development.</param>
        /// <param name="revTs">Revision timestamp.</param>
        /// <param name="revHash">Revision hash.</param>
        public IgniteProductVersion(byte major, byte minor, byte maintenance, String stage, long revTs, byte[] revHash)
        {
            if (revHash != null && revHash.Length != 20)
                throw new ArgumentException("Invalid length for SHA1 hash (must be 20): " + revHash.Length);

            _major = major;
            _minor = minor;
            _maintenance = maintenance;
            _stage = stage;
            _revTs = revTs;
            _revHash = revHash ?? new byte[20];
        }

        /// <summary>
        /// Constructor.
        /// </summary>
        /// <param name="reader"><see cref="IBinaryRawReader"/></param>
        public IgniteProductVersion(IBinaryRawReader reader)
        {
            IgniteArgumentCheck.NotNull(reader, "reader");

            _major = reader.ReadByte();
            _minor = reader.ReadByte();
            _maintenance = reader.ReadByte();
            _revTs = reader.ReadLong();
            _revHash = reader.ReadByteArray();
        }

        /// <summary>
        /// Gets the major version number.
        /// </summary>
        public byte Major
        {
            get { return _major; }
        }

        /// <summary>
        /// Gets the minor version number.
        /// </summary>
        public byte Minor
        {
            get { return _minor; }
        }

        /// <summary>
        /// Gets the maintenance version number.
        /// </summary>
        public byte Maintenance
        {
            get { return _maintenance; }
        }

        /// <summary>
        /// Gets the stage of development.
        /// </summary>
        public string Stage
        {
            get { return _stage; }
        }

        /// <summary>
        /// Gets the revision timestamp.
        /// </summary>
        public long RevisionTimestamp
        {
            get { return _revTs; }
        }

        /// <summary>
        /// Gets the revision hash.
        /// </summary>
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Performance", "CA1819:PropertiesShouldNotReturnArrays")]
        public byte[] RevisionHash
        {
            get { return _revHash;}
        }

        /// <summary>
        /// Gets the release date.
        /// </summary>
        public DateTime ReleaseDate
        {
            get { return new DateTime(_revTs * 1000); }
        }

        /** <inheritDoc /> */
        public override int GetHashCode()
        {
            int res = _major;

            res = 31 * res + _minor;
            res = 31 * res + _maintenance;
            res = 31 * res + (int)(_revTs ^ (_revTs >> 32));

            return res;
        }

        /** <inheritDoc /> */
        public override string ToString()
        {
            String revTsStr = ReleaseDate.ToString("yyyyMMdd");

            return string.Format("{0}.{1}.{2}#{3}", Major, Minor, Maintenance, revTsStr);
        }
        
        /** <inheritDoc /> */
        public bool Equals(IgniteProductVersion other)
        {
            if (other == null)
                return false;

            return RevisionTimestamp == other.RevisionTimestamp 
                   && Maintenance == other.Maintenance
                   && Minor == other.Minor
                   && Major == other.Major;
        }

        /** <inheritDoc /> */
        public int CompareTo(IgniteProductVersion other)
        {
            // NOTE: Unknown version is less than any other version.
            int res = Major.CompareTo(other.Major);

            if (res != 0)
                return res;

            res = Minor.CompareTo(other.Minor);

            if (res != 0)
                return res;

            res = Maintenance.CompareTo(other.Maintenance);

            if (res != 0)
                return res;

            return RevisionTimestamp.CompareTo(other.RevisionTimestamp);
        }
    }
}
