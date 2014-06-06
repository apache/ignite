/* @csharp.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

namespace GridGain.Client.Impl.Message {
    using System;

    /** <summary>Task result.</summary> */
    internal class GridClientTaskResultBean : IGridPortableObject {
        public const int PORTABLE_TYPE_ID = -8;
        
        /** <summary>Synthetic ID containing task ID and result holding node ID.</summary> */
        public String TaskId {
            get;
            set;
        }

        /** <summary>Execution finished flag.</summary> */
        public bool IsFinished {
            get;
            set;
        }

        /** <summary>Task result.</summary> */
        public Object Result {
            get;
            set;
        }

        /** <summary>Error if any occurs while execution.</summary> */
        public String Error {
            get;
            set;
        }

        public int TypeId {
            get { return PORTABLE_TYPE_ID;  } 
        }
        
        public void WritePortable(IGridPortableWriter writer) {
            writer.WriteString("id", TaskId);
            writer.WriteBoolean("finished", IsFinished);
            writer.WriteObject("res", Result);
            writer.WriteString("error", Error);
        }

        public void ReadPortable(IGridPortableReader reader) {
            TaskId = reader.ReadString("id");
            IsFinished = reader.ReadBoolean("finished");
            Result = reader.ReadObject<Object>("res");
            Error = reader.ReadString("error");
        }
    }
}
