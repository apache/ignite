// @csharp.file.header

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

namespace GridGain.Client {
    using System;

    /** <summary>Cache metrics used to obtain statistics on cache itself or any of its entries.</summary> */
    public interface IGridClientDataMetrics {
        /** <summary>Gets create time of the owning entity (either cache or entry).</summary> */
        DateTime CreateTime {
            get;
        }

        /** <summary>Gets last write time of the owning entity (either cache or entry).</summary> */
        DateTime WriteTime {
            get;
        }

        /** <summary>Gets last read time of the owning entity (either cache or entry).</summary> */
        DateTime ReadTime {
            get;
        }

        /** <summary>Gets total number of reads of the owning entity (either cache or entry).</summary> */
        long Reads {
            get;
        }

        /** <summary>Gets total number of writes of the owning entity (either cache or entry).</summary> */
        long Writes {
            get;
        }

        /** <summary>Gets total number of hits for the owning entity (either cache or entry).</summary> */
        long Hits {
            get;
        }

        /** <summary>Gets total number of misses for the owning entity (either cache or entry).</summary> */
        long Misses {
            get;
        }
    }
}