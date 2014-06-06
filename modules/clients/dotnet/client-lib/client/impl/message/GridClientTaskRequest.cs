/* @csharp.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

namespace GridGain.Client.Impl.Message {
    using System;

    using U = GridGain.Client.Util.GridClientUtils;

    /** <summary><c>Task</c> command request.</summary> */
    internal class GridClientTaskRequest : GridClientRequest {
        public const int PORTABLE_TYPE_ID = -7;
        
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

        public override int TypeId {
            get { return PORTABLE_TYPE_ID; }
        }

        public override void WritePortable(IGridPortableWriter writer) {
            base.WritePortable(writer);

            writer.WriteString("taskName", TaskName);

            writer.WriteObject("arg", Argument);
        }

        public override void ReadPortable(IGridPortableReader reader) {
            base.ReadPortable(reader);

            TaskName = reader.ReadString("taskName");

            Argument = reader.ReadObject<Object>("arg");
        }
    }
}
