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
    /// Employee key. Used in query example to co-locate employees with their organizations.
    /// </summary>
    [Serializable]
    public class EmployeeKey
    {
        /// <summary>
        /// Constructor.
        /// </summary>
        /// <param name="id">ID.</param>
        /// <param name="orgId">Organization ID.</param>
        public EmployeeKey(int id, int orgId)
        {
            Id = id;
            OrganizationId = orgId;
        }

        /// <summary>
        /// ID.
        /// </summary>
        public int Id { get; private set; }

        /// <summary>
        /// Organization ID.
        /// </summary>
        public int OrganizationId { get; private set; }
        
        /// <summary>
        /// Determines whether the specified <see cref="T:System.Object"/> is equal to the current <see cref="T:System.Object"/>.
        /// </summary>
        /// <returns>
        /// true if the specified <see cref="T:System.Object"/> is equal to the current <see cref="T:System.Object"/>; otherwise, false.
        /// </returns>
        /// <param name="obj">The object to compare with the current object. </param><filterpriority>2</filterpriority>
        public override bool Equals(object obj)
        {
            EmployeeKey other = obj as EmployeeKey;

            return other != null && Id == other.Id && OrganizationId == other.OrganizationId;
        }

        /// <summary>
        /// Serves as a hash function for a particular type. 
        /// </summary>
        /// <returns>
        /// A hash code for the current <see cref="T:System.Object"/>.
        /// </returns>
        /// <filterpriority>2</filterpriority>
        public override int GetHashCode()
        {
            return 31 * Id + OrganizationId;
        }

        /// <summary>
        /// Returns a string that represents the current object.
        /// </summary>
        /// <returns>
        /// A string that represents the current object.
        /// </returns>
        public override string ToString()
        {
            return string.Format("{0} [id={1}, organizationId={2}]", typeof (EmployeeKey).Name, Id, OrganizationId);
        }
    }
}
