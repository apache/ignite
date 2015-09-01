/*
 *  Copyright (C) GridGain Systems. All Rights Reserved.
 *  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

namespace GridGain.Portable
{
    using System;
    using System.Collections.Generic;

    /// <summary>
    /// Defines portable objects functionality. With portable objects you are able to:
    /// <list type="bullet">
    ///     <item>
    ///         <description>Seamlessly interoperate between Java, .NET, and C++.</description>
    ///     </item>
    ///     <item>
    ///         <description>Make any object portable with zero code change to your existing code.</description>
    ///     </item>
    ///     <item>
    ///         <description>Nest portable objects within each other.</description>
    ///     </item>
    ///     <item>
    ///         <description>Automatically handle <c>circular</c> or <c>null</c> references.</description>
    ///     </item>
    ///     <item>
    ///         <description>Automatically convert collections and maps between Java, .NET, and C++.</description>
    ///     </item>
    ///     <item>
    ///         <description>Optionally avoid deserialization of objects on the server side.</description>
    ///     </item>
    ///     <item>
    ///         <description>Avoid need to have concrete class definitions on the server side.</description>
    ///     </item>
    ///     <item>
    ///         <description>Dynamically change structure of the classes without having to restart the cluster.</description>
    ///     </item>
    ///     <item>
    ///         <description>Index into portable objects for querying purposes.</description>
    ///     </item>
    /// </list>
    /// </summary>
    public interface IPortables
    {
        /// <summary>
        /// Converts provided object to portable form.
        /// <para />
        /// Note that object's type needs to be configured in <see cref="PortableConfiguration"/>.
        /// </summary>
        /// <param name="obj">Object to convert.</param>
        /// <returns>Converted object.</returns>
        T ToPortable<T>(object obj);

        /// <summary>
        /// Create builder for the given portable object type. Note that this
        /// type must be specified in <see cref="PortableConfiguration"/>.
        /// </summary>
        /// <param name="type"></param>
        /// <returns>Builder.</returns>
        IPortableBuilder Builder(Type type);

        /// <summary>
        /// Create builder for the given portable object type name. Note that this
        /// type name must be specified in <see cref="PortableConfiguration"/>.
        /// </summary>
        /// <param name="typeName">Type name.</param>
        /// <returns>Builder.</returns>
        IPortableBuilder Builder(string typeName);

        /// <summary>
        /// Create builder over existing portable object.
        /// </summary>
        /// <param name="obj"></param>
        /// <returns>Builder.</returns>
        IPortableBuilder Builder(IPortableObject obj);

        /// <summary>
        /// Gets type id for the given type name.
        /// </summary>
        /// <param name="typeName">Type name.</param>
        /// <returns>Type id.</returns>
        int GetTypeId(string typeName);

        /// <summary>
        /// Gets metadata for all known types.
        /// </summary>
        /// <returns>Metadata.</returns>
        ICollection<IPortableMetadata> GetMetadata();

        /// <summary>
        /// Gets metadata for specified type id.
        /// </summary>
        /// <returns>Metadata.</returns>
        IPortableMetadata GetMetadata(int typeId);

        /// <summary>
        /// Gets metadata for specified type name.
        /// </summary>
        /// <returns>Metadata.</returns>
        IPortableMetadata GetMetadata(string typeName);

        /// <summary>
        /// Gets metadata for specified type.
        /// </summary>
        /// <returns>Metadata.</returns>
        IPortableMetadata GetMetadata(Type type);
    }
}
