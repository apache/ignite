/* @cpp.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

#ifndef GRID_CLIENT_DATA_METRICS_HPP_INCLUDED
#define GRID_CLIENT_DATA_METRICS_HPP_INCLUDED

#include <iostream>

#include <gridgain/gridconf.hpp>

/**
 * Cache metrics used to obtain statistics on cache itself or any of its entries.
 */
class GRIDGAIN_API GridClientDataMetrics {
public:
    /** Default constructor. */
    GridClientDataMetrics();

    /**
     * Copy constructor.
     *
     * @param other Metrics to copy data from.
     */
    GridClientDataMetrics(const GridClientDataMetrics& other);

    /**
     * Assignment operator override.
     *
     * @param rhs Right-hand side of the assignment operator.
     * @return This instance of the class.
     */
    GridClientDataMetrics& operator=(const GridClientDataMetrics& rhs);

    /** Destructor. */
    virtual ~GridClientDataMetrics();

    /**
    * Gets create time of the owning entity (either cache or entry).
    *
    * @return Create time.
    */
    int64_t createTime() const;

    /**
    * Gets last write time of the owning entity (either cache or entry).
    *
    * @return Last write time.
    */
    int64_t writeTime() const;

    /**
    * Gets last read time of the owning entity (either cache or entry).
    *
    * @return Last read time.
    */
    int64_t readTime() const;

    /**
    * Gets total number of reads of the owning entity (either cache or entry).
    *
    * @return Total number of reads.
    */
    int32_t reads() const;

    /**
    * Gets total number of writes of the owning entity (either cache or entry).
    *
    * @return Total number of writes.
    */
    int32_t writes() const;

    /**
    * Gets total number of hits for the owning entity (either cache or entry).
    *
    * @return Number of hits.
    */
    int32_t hits() const;

    /**
    * Gets total number of misses for the owning entity (either cache or entry).
    *
    * @return Number of misses.
    */
    int32_t misses() const;

     /**
     * Sets creation time.
     *
     * @param createTime Creation time.
     */
    void createTime(int64_t createTime);

    /**
     * Sets read time.
     *
     * @param readTime Read time.
     */
    void readTime(int64_t readTime);

    /**
     * Sets write time.
     *
     * @param writeTime Write time.
     */
    void writeTime(int64_t writeTime);

    /**
     * Sets number of reads.
     *
     * @param reads Number of reads.
     */
    void reads(int32_t reads);

    /**
     * Sets number of writes.
     *
     * @param writes Number of writes.
     */
    void writes(int32_t writes);

    /**
     * Sets number of hits.
     *
     * @param hits Number of hits.
     */
    void hits(int32_t hits);

    /**
     * Sets number of misses.
     *
     * @param misses Number of misses.
     */
    void misses(int32_t misses);

private:
    class Impl;
    Impl* pimpl;
};

/**
 * Prints metrics to the output stream
 *
 * @param out Stream to output metrics to.
 * @param m Data metrics.
 */
inline std::ostream& operator<< (std::ostream &out, const GridClientDataMetrics &m) {
    return out << "GridClientDataMetrics [createTime=" << m.createTime() << ", readTime=" << m.readTime() <<
            ", writeTime=" << m.writeTime() << ", reads=" << m.reads() << ", writes=" << m.writes() <<
            ", hits=" << m.hits() << ", misses=" << m.misses() << ']';
}

#endif
