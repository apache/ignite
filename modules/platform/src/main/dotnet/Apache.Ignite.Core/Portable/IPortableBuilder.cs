/*
 *  Copyright (C) GridGain Systems. All Rights Reserved.
 *  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

namespace Apache.Ignite.Core.Portable
{
    using System.Diagnostics.CodeAnalysis;

    /// <summary>
    /// Portable object builder. Provides ability to build portable objects dynamically
    /// without having class definitions.
    /// <para />
    /// Note that type ID is required in order to build portable object. Usually it is
    /// enough to provide a simple type name and GridGain will generate the type ID
    /// automatically.
    /// </summary>
    public interface IPortableBuilder
    {
        /// <summary>
        /// Get object field value. If value is another portable object, then
        /// builder for this object will be returned. If value is a container
        /// for other objects (array, ICollection, IDictionary), then container
        /// will be returned with primitive types in deserialized form and
        /// portable objects as builders. Any change in builder or collection
        /// returned through this method will be reflected in the resulting
        /// portable object after build.
        /// </summary>
        /// <param name="fieldName">Field name.</param>
        /// <returns>Field value.</returns>
        T GetField<T>(string fieldName);

        /// <summary>
        /// Set object field value. Value can be of any type including other
        /// <see cref="IPortableObject"/> and other builders.
        /// </summary>
        /// <param name="fieldName">Field name.</param>
        /// <param name="val">Field value.</param>
        /// <returns>Current builder instance.</returns>
        IPortableBuilder SetField<T>(string fieldName, T val);

        /// <summary>
        /// Remove object field.
        /// </summary>
        /// <param name="fieldName">Field name.</param>
        /// <returns>Current builder instance.</returns>
        IPortableBuilder RemoveField(string fieldName);

        /// <summary>
        /// Set explicit hash code. If builder creating object from scratch,
        /// then hash code initially set to 0. If builder is created from
        /// exising portable object, then hash code of that object is used
        /// as initial value.
        /// </summary>
        /// <param name="hashCode">Hash code.</param>
        /// <returns>Current builder instance.</returns>
        [SuppressMessage("Microsoft.Naming", "CA1719:ParameterNamesShouldNotMatchMemberNames", MessageId = "0#")]
        IPortableBuilder HashCode(int hashCode);

        /// <summary>
        /// Build the object.
        /// </summary>
        /// <returns>Resulting portable object.</returns>
        IPortableObject Build();
    }
}
