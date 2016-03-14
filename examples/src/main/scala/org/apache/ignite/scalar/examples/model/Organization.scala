/*
 * ___    _________________________ ________
 * __ |  / /____  _/__  ___/__  __ \___  __ \
 * __ | / /  __  /  _____ \ _  / / /__  /_/ /
 * __ |/ /  __/ /   ____/ / / /_/ / _  _, _/
 * _____/   /___/   /____/  \____/  /_/ |_|
 *
 */

package org.apache.ignite.scalar.examples.model

import java.sql.Timestamp
import java.util.concurrent.atomic.AtomicLong

import org.apache.ignite.cache.query.annotations.QuerySqlField
import org.apache.ignite.examples.model.{Address, OrganizationType}

class Organization {
    /** */
    private[this] val ID_GEN = new AtomicLong

    /** Organization ID (indexed). */
    @QuerySqlField(index = true) private[this] var id = 0L

    /** Organization name (indexed). */
    @QuerySqlField(index = true) private[this] var name: String = null

    /** Address. */
    private[this] var addr: Address = null

    /** Type. */
    private[this] var orgType: OrganizationType = null

    /** Last update time. */
    private[this] var lastUpdated: Timestamp = null

    /**
      * @param name Organization name.
      */
    def this(name: String) {
        this()

        id = ID_GEN.incrementAndGet
        this.name = name
    }

    /**
      * @param name Name.
      * @param addr Address.
      * @param type Type.
      * @param lastUpdated Last update time.
      */
    def this(name: String, addr: Address, `type`: OrganizationType, lastUpdated: Timestamp) {
        this()

        id = ID_GEN.incrementAndGet
        this.name = name
        this.addr = addr
        this.orgType = `type`
        this.lastUpdated = lastUpdated
    }

    /**
      * @return Organization ID.
      */
    def getId: Long = {
        id
    }

    /**
      * @return Name.
      */
    def getName: String = {
        name
    }

    /**
      * @return Address.
      */
    def getAddress: Address = {
        addr
    }

    /**
      * @return Type.
      */
    def getOrganizationType: OrganizationType = {
        orgType
    }

    /**
      * @return Last update time.
      */
    def getLastUpdated: Timestamp = {
        lastUpdated
    }

    /** @inheritdoc */
    override def toString: String = {
        "Organization [id=" + id +
            ", name=" + name +
            ", address=" + addr +
            ", type=" + orgType +
            ", lastUpdated=" + lastUpdated +
            ']'
    }
}
