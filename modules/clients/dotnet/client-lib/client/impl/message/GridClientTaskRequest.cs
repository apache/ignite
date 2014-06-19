/* @csharp.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

namespace GridGain.Client.Impl.Message {
    using System;
    using GridGain.Client.Portable;

    using PU = GridGain.Client.Impl.Portable.GridClientPortableUilts;
    using U = GridGain.Client.Util.GridClientUtils;

    /** <summary><c>Task</c> command request.</summary> */
    [GridClientPortableId(PU.TYPE_TASK_REQ)]
    internal class GridClientTaskRequest : GridClientRequest {
        /**
         * <summary>
         * Constructs task command request.</summary>
         * 
         * <param name="destNodeId">Node ID to route request to.</param>
         */
        public GridClientTaskRequest(Guid destNodeId) : base(destNodeId) { 
        }

        /** <summary>Task name.</summary> */
        public String TaskName {
            get;
            set;
        }

        /** <summary>Task argument.</summary> */
        public Object Argument {
            get;
            set;
        }

        /** <inheritdoc /> */
        public override void WritePortable(IGridClientPortableWriter writer) {
            base.WritePortable(writer);

            writer.WriteString(TaskName);

            writer.WriteObject(Argument);
        }

        /** <inheritdoc /> */
        public override void ReadPortable(IGridClientPortableReader reader) {
            base.ReadPortable(reader);

            TaskName = reader.ReadString();

            Argument = reader.ReadObject<Object>();
        }
    }
}
