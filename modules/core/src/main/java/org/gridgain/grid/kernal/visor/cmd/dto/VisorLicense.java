/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.visor.cmd.dto;

import org.gridgain.grid.kernal.GridEx;
import org.gridgain.grid.product.GridProductLicense;

import java.io.Serializable;
import java.util.*;

/**
 * Data transfer object for grid license properties.
 */
public class VisorLicense implements Serializable {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    public static final String HPC_PRODUCT = "hpc";

    /** */
    public static final String DATA_GRID_PRODUCT = "datagrid";

    /** */
    public static final String HADOOP_PRODUCT = "hadoop";

    /** */
    public static final String STREAMING_PRODUCT = "streaming";

    /** */
    public static final String MONGO_PRODUCT = "mongo";

    /** License ID. */
    private final UUID id;

    /** License version. */
    private final String version;

    /** GridGain version regular expression. */
    private final String versionRegexp;

    /** Issue organization. */
    private final String issueOrganization;

    /** User name. */
    private final String userName;

    /** User organization. */
    private final String userOrganization;

    /** User web URL. */
    private final String userWww;

    /** User email. */
    private final String userEmail;

    /** License note. */
    private final String note;

    /** Expiration date. */
    private final Date expireDate;

    /** Issue date. */
    private final Date issueDate;

    /** Maintenance Time. */
    private final int maintenanceTime;

    /** Maximum nodes. */
    private final int maxNodes;

    /** Maximum computers. */
    private final int maxComputers;

    /** Maximum CPUs. */
    private final int maxCpus;

    /** Maximum up-time. */
    private final long maxUpTime;

    /** Maximum grace period in minutes. */
    private final long gracePeriod;

    /** Attribute name. */
    private final String attributeName;

    /** Attribute value. */
    private final String attributeValue;

    /** Disabled sub-systems. */
    private final String disabledSubsystems;

    /** Grace period left in minutes if bursting or `-1` otherwise. */
    private final long gracePeriodLeft;

    /** Create license with given parameters. */
    public VisorLicense(
        UUID id,
        String version,
        String versionRegexp,
        String issueOrganization,
        String userName,
        String userOrganization,
        String userWww,
        String userEmail,
        String note,
        Date expireDate,
        Date issueDate,
        int maintenanceTime,
        int maxNodes,
        int maxComputers,
        int maxCpus,
        long maxUpTime,
        long gracePeriod,
        String attributeName,
        String attributeValue,
        String disabledSubsystems,
        long gracePeriodLeft
    ) {
        this.id = id;
        this.version = version;
        this.versionRegexp = versionRegexp;
        this.issueOrganization = issueOrganization;
        this.userName = userName;
        this.userOrganization = userOrganization;
        this.userWww = userWww;
        this.userEmail = userEmail;
        this.note = note;
        this.expireDate = expireDate;
        this.issueDate = issueDate;
        this.maintenanceTime = maintenanceTime;
        this.maxNodes = maxNodes;
        this.maxComputers = maxComputers;
        this.maxCpus = maxCpus;
        this.maxUpTime = maxUpTime;
        this.gracePeriod = gracePeriod;
        this.attributeName = attributeName;
        this.attributeValue = attributeValue;
        this.disabledSubsystems = disabledSubsystems;
        this.gracePeriodLeft = gracePeriodLeft;
    }

    /** Create data transfer object for given license. */
    public static VisorLicense create(GridEx g) {
        assert g != null;

        GridProductLicense lic = g.product().license();

        assert lic != null;

        return new VisorLicense(
            lic.id(),
            lic.version(),
            lic.versionRegexp(),
            lic.issueOrganization(),
            lic.userName(),
            lic.userOrganization(),
            lic.userWww(),
            lic.userEmail(),
            lic.licenseNote(),
            lic.expireDate(),
            lic.issueDate(),
            lic.maintenanceTime(),
            lic.maxNodes(),
            lic.maxComputers(),
            lic.maxCpus(),
            lic.maxUpTime(),
            lic.gracePeriod(),
            lic.attributeName(),
            lic.attributeValue(),
            lic.disabledSubsystems(),
            g.licenseGracePeriodLeft()
        );
    }

    /**
     * @return License ID.
     */
    public UUID id() {
        return id;
    }

    /**
     * @return License version.
     */
    public String version() {
        return version;
    }

    /**
     * @return GridGain version regular expression.
     */
    public String versionRegexp() {
        return versionRegexp;
    }

    /**
     * @return Issue organization.
     */
    public String issueOrganization() {
        return issueOrganization;
    }

    /**
     * @return User name.
     */
    public String userName() {
        return userName;
    }

    /**
     * @return User organization.
     */
    public String userOrganization() {
        return userOrganization;
    }

    /**
     * @return User web URL.
     */
    public String userWww() {
        return userWww;
    }

    /**
     * @return User email.
     */
    public String userEmail() {
        return userEmail;
    }

    /**
     * @return License note.
     */
    public String note() {
        return note;
    }

    /**
     * @return Expiration date.
     */
    public Date expireDate() {
        return expireDate;
    }

    /**
     * @return Issue date.
     */
    public Date issueDate() {
        return issueDate;
    }

    /**
     * @return Maintenance Time.
     */
    public int maintenanceTime() {
        return maintenanceTime;
    }

    /**
     * @return Maximum nodes.
     */
    public int maxNodes() {
        return maxNodes;
    }

    /**
     * @return Maximum computers.
     */
    public int maxComputers() {
        return maxComputers;
    }

    /**
     * @return Maximum CPUs.
     */
    public int maxCpus() {
        return maxCpus;
    }

    /**
     * @return Maximum up-time.
     */
    public long maxUpTime() {
        return maxUpTime;
    }

    /**
     * @return Maximum grace period in minutes.
     */
    public long gracePeriod() {
        return gracePeriod;
    }

    /**
     * @return Attribute name.
     */
    public String attributeName() {
        return attributeName;
    }

    /**
     * @return Attribute value.
     */
    public String attributeValue() {
        return attributeValue;
    }

    /**
     * @return Disabled sub-systems.
     */
    public String disabledSubsystems() {
        return disabledSubsystems;
    }

    /**
     * @return Grace period left in minutes if bursting or `-1` otherwise.
     */
    public long gracePeriodLeft() {
        return gracePeriodLeft;
    }
}
