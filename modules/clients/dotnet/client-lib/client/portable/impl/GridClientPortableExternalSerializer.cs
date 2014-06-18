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
     * <summary>Serializer capable of writing portable objects.</summary>
     */
    class GridClientPortableExternalSerializer : IGridClientPortableSerializer
    {
        /** <inheritdoc /> */
        public void WritePortable(object obj, IGridClientPortableWriter writer)
        {
            if (obj is IGridClientPortableEx)
            {
                IGridClientPortableEx obj0 = (IGridClientPortableEx)obj;

                obj0.WritePortable(writer);
            }
            else
                throw new GridClientPortableInvalidClassException("Class being marshalled doesn't implement external portable interface: " + obj.GetType());
        }

        /** <inheritdoc /> */
        public T ReadPortable<T>(IGridClientPortableReader reader)
        {
            throw new NotImplementedException();
        }
    }
}
