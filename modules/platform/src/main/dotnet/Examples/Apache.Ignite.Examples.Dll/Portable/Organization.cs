/*
 *  Copyright (C) GridGain Systems. All Rights Reserved.
 *  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

using System;

namespace GridGain.Examples.Portable
{
    /// <summary>
    /// Organization.
    /// </summary>
    [Serializable]
    public class Organization
    {
        /// <summary>
        /// Default constructor.
        /// </summary>
        public Organization()
        {
            // No-op.
        }

        /// <summary>
        /// Constructor.
        /// </summary>
        /// <param name="name">Name.</param>
        /// <param name="address">Address.</param>
        /// <param name="type">Type.</param>
        /// <param name="lastUpdated">Last update time.</param>
        public Organization(string name, Address address, OrganizationType type, DateTime lastUpdated)
        {
            Name = name;
            Address = address;
            Type = type;
            LastUpdated = lastUpdated;
        }

        /// <summary>
        /// Name.
        /// </summary>
        public string Name
        {
            get;
            set;
        }

        /// <summary>
        /// Address.
        /// </summary>
        public Address Address
        {
            get;
            set;
        }

        /// <summary>
        /// Type.
        /// </summary>
        public OrganizationType Type
        {
            get;
            set;
        }

        /// <summary>
        /// Last update time.
        /// </summary>
        public DateTime LastUpdated
        {
            get;
            set;
        }
        
        /// <summary>
        /// Returns a string that represents the current object.
        /// </summary>
        /// <returns>
        /// A string that represents the current object.
        /// </returns>
        /// <filterpriority>2</filterpriority>
        public override string ToString()
        {
            return string.Format("{0} [name={1}, address={2}, type={3}, lastUpdated={4}]", typeof(Organization).Name,
                Name, Address, Type, LastUpdated);
        }
    }
}
