/* @csharp.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

namespace GridGain.Client.Impl {
    using System;

    /** <summary>Base implementation for cache metrics.</summary> */
    internal class GridClientDataMetrics : IGridClientDataMetrics {
        /** <summary>Constructs empty cache metrics.</summary> */
        public GridClientDataMetrics() {
            var now = DateTime.Now;

            CreateTime = now;
            WriteTime = now;
            ReadTime = now;
        }

        /** <summary>Gets create time of the owning entity (either cache or entry).</summary> */
        public DateTime CreateTime {
            get;
            set;
        }

        /** <summary>Gets last write time of the owning entity (either cache or entry).</summary> */
        public DateTime WriteTime {
            get;
            set;
        }

        /** <summary>Gets last read time of the owning entity (either cache or entry).</summary> */
        public DateTime ReadTime {
            get;
            set;
        }

        /** <summary>Gets total number of reads of the owning entity (either cache or entry).</summary> */
        public long Reads {
            get;
            set;
        }

        /** <summary>Gets total number of writes of the owning entity (either cache or entry).</summary> */
        public long Writes {
            get;
            set;
        }

        /** <summary>Gets total number of hits for the owning entity (either cache or entry).</summary> */
        public long Hits {
            get;
            set;
        }

        /** <summary>Gets total number of misses for the owning entity (either cache or entry).</summary> */
        public long Misses {
            get;
            set;
        }

        /** <inheritDoc/> */
        override public String ToString() {
            return "GridClientDataMetrics [" +
                "createTime=" + CreateTime +
                ", hits=" + Hits +
                ", misses=" + Misses +
                ", reads=" + Reads +
                ", readTime=" + ReadTime +
                ", writes=" + Writes +
                ", writeTime=" + WriteTime +
                ']';
        }
    }
}
