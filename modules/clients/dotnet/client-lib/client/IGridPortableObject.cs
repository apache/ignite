/* @csharp.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

namespace GridGain.Client {
    using System;

    /// <summary>
    /// 
    /// </summary>
    public interface IGridPortableObject {
        /// <summary>
        /// 
        /// </summary>
        int TypeId { get; }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="writer"></param>
        void WritePortable(IGridPortableWriter writer);

        /// <summary>
        /// 
        /// </summary>
        /// <param name="reader"></param>
        void ReadPortable(IGridPortableReader reader);
    }
}