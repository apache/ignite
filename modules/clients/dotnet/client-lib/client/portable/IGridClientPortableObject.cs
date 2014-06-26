/* @csharp.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

namespace GridGain.Client.Portable
{
    using System;
    using System.Collections.Generic;

    /**
     * <summary>Wrapper for serialized portable objects.</summary>
     */
    public interface IGridClientPortableObject {
        /**
         * <summary>Gets hash code.</summary> 
         * <returns>Hash code.</returns>
         */
        int HashCode();
        
        /**
         * <summary>Checks whether object type ID is user type.</summary> 
         * <returns>Type ID.</returns>
         */
        bool IsUserType();

        /**
         * <summary>Gets portable object type ID.</summary> 
         * <returns>Type ID.</returns>
         */
        int TypeId();
                        
        /**
         * <summary>Gets field value.</summary>
         * <param name="fieldName">Field name.</param>
         * <returns>Field value.</returns>
         */
        F Field<F>(string fieldName);

        /**
         * <summary>Gets fully deserialized instance of portable object.</summary>
         * <returns>Fully deserialized instance of portable object.</returns>
         */
        T Deserialize<T>();

        /**
         * <summary>Creates a copy of this portable object and optionally changes field values
         * if they are provided in map. If map is empty or null, clean copy is created.</summary>
         * <param name="fields">Fields to modify in copy.</param>
         * <returns>Copy of this portable object.</returns>
         */
        IGridClientPortableObject Copy(IDictionary<string, object> fields);        
    }
}