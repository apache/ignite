/* @csharp.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

namespace GridGain.Client.Portable
{
    /**
     * <summary>Maps class name and class field names to integer identifiers.</summary>
     */ 
    public abstract class GridClientPortableIdResolver
    {
        /**
         * <summary>Gets class ID for the given class.</summary>
         * <param name="className">Class name.</param>
         * <returns>ID of the class or null in case annotation or hash code is to be used.</returns>
         */
        public virtual int? TypeId(string className)
        {
            return null;
        }

        /**
         * <summary>Gets field ID for the given field of the given class.</summary>
         * <param name="typeId">Type ID.</param>
         * <param name="fieldName">Field name.</param>
         * <returns>ID of the field or null in case annotation or hash code is to be used.</returns>
         */
        public virtual int? FieldId(int typeId, string fieldName)
        {
            return null;
        }
    }
}
