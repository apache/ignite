/* @cpp.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

#ifndef GRID_DATE_HPP_INCLUDED
#define GRID_DATE_HPP_INCLUDED

#include <iostream>

#include <gridgain/gridhasheableobject.hpp>

/**
 */
class GRIDGAIN_API GridClientDate : public GridClientHasheableObject {
public:
    GridClientDate(int64_t time) : time(time), nanoTicks(0) {
    }

    GridClientDate(int64_t time, int16_t nanoTicks) : time(time), nanoTicks(nanoTicks) {
    }

    /**
     * Copy constructor.
     *
     * @param other Another instance of date.
     */
    GridClientDate(const GridClientDate& other) : time(other.time), nanoTicks(other.nanoTicks) {
    }

    /**
     * Assignment operator override.
     *
     * @param rhs Right-hand side of the assignment operator.
     * @return This instance of the class.
     */
    GridClientDate& operator=(const GridClientDate& other) {
        time = other.time;
        nanoTicks = other.nanoTicks;

        return *this;
    }

    /**
     * Comparison operator for date.
     *
     * @param other date to compare this date to.
     * @return <tt>true</tt> if this date is less than other, <tt>false</tt> otherwise.
     */
    bool operator <(const GridClientDate& other) const {
        return time < other.time;
    }

    /**
     * Comparison operator for date.
     *
     * @param other date to compare this date to.
     * @return <tt>true</tt> if this date equals another, <tt>false</tt> otherwise.
     */
    bool operator ==(const GridClientDate& other) const {
        return time == other.time && nanoTicks == other.nanoTicks;
    }

    int64_t getTime() const {
        return time;
    }

    int16_t getNanoTicks() const {
        return nanoTicks;
    }

    /**
     * Returns hash code for this date following Java conventions.
     *
     * @return Hash code.
     */
    int32_t hashCode() const {
        return (int32_t)time ^ (int32_t)(time >> 32);
    }

private:
    /** */
    int64_t time;

    /** */
    int16_t nanoTicks;

    /**
     * Prints date to stream
     *
     * @param out Stream to output date to.
     * @param val date.
     */
    friend std::ostream& operator<<(std::ostream &out, const GridClientDate& val);
};

inline std::ostream& operator<<(std::ostream &out, const GridClientDate& val) {
    return out << "Date [time=" << val.time << ", nanoTicks=" << val.nanoTicks << "]";
}

#endif
