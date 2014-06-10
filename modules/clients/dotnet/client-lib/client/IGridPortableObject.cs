/* @csharp.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

namespace GridGain.Client {
    using System;
    using System.Collections.Generic;

    /**
     * <summary>Wrapper for serialized portable objects.</summary>
     */
    [Serializable]
    public interface IGridPortableObject {
        /**
         * <summary>Gets portable object type ID.</summary> 
         * <returns>Type ID.</returns>
         */
        int TypeId { get; }

        /**
         * <summary>Gets portable object type name.</summary>
         * <returns>Type name.</returns>
         */
        string typeName();

        /**
         * <summary>Gets list of field names that are accessible in this portable object.</summary>
         * <returns>Field names.</returns>
         */
        ICollection<string> fields();

        /**
         * <summary>Gets field value.</summary>
         * <param name="fieldName">Field name.</param>
         * <returns>Field value.</returns>
         */
        F field<F>(string fieldName);

        /**
         * <summary>Creates a copy of this portable object and optionally changes field values
         * if they are provided in map. If map is empty or {@code null}, clean copy
         * is created.</summary>
         * <param name="fields">Fields to modify in copy.</param>
         * <returns>Copy of this portable object.</returns>
         */
        IGridPortableObject copy(IDictionary<string, object> fields);

        /**
         * <summary>Gets fully deserialized instance of portable object.</summary>
         * <returns>Fully deserialized instance of portable object.</returns>
         */
        T deserialize<T>() where T : IGridPortable;
    }
}