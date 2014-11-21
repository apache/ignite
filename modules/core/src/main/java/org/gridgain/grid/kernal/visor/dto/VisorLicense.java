/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.visor.dto;

import org.gridgain.grid.kernal.*;
import org.gridgain.grid.product.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.jetbrains.annotations.*;

import java.io.*;
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
    private UUID id;

    /** License version. */
    private String version;

    /** GridGain version regular expression. */
    private String versionRegexp;

    /** Issue organization. */
    private String issueOrganization;

    /** User name. */
    private String userName;

    /** User organization. */
    private String userOrganization;

    /** User web URL. */
    private String userWww;

    /** User email. */
    private String userEmail;

    /** License note. */
    private String note;

    /** Expiration date. */
    private Date expireDate;

    /** Issue date. */
    private Date issueDate;

    /** Maintenance Time. */
    private int maintenanceTime;

    /** Maximum nodes. */
    private int maxNodes;

    /** Maximum computers. */
    private int maxComputers;

    /** Maximum CPUs. */
    private int maxCpus;

    /** Maximum up-time. */
    private long maxUpTime;

    /** Maximum grace period in minutes. */
    private long gracePeriod;

    /** Attribute name. */
    private String attributeName;

    /** Attribute value. */
    private String attributeValue;

    /** Disabled sub-systems. */
    private String disabledSubsystems;

    /** Grace period left in minutes if bursting or {@code -1} otherwise. */
    private long gracePeriodLeft;

    /**
     * @param g Grid.
     * @return Data transfer object for grid license properties.
     */
    @Nullable public static VisorLicense from(GridEx g) {
        assert g != null;

        GridProductLicense lic = g.product().license();

        if (lic == null)
            return null;

        VisorLicense l = new VisorLicense();

        l.id(lic.id());
        l.version(lic.version());
        l.versionRegexp(lic.versionRegexp());
        l.issueOrganization(lic.issueOrganization());
        l.userName(lic.userName());
        l.userOrganization(lic.userOrganization());
        l.userWww(lic.userWww());
        l.userEmail(lic.userEmail());
        l.note(lic.licenseNote());
        l.expireDate(lic.expireDate());
        l.issueDate(lic.issueDate());
        l.maintenanceTime(lic.maintenanceTime());
        l.maxNodes(lic.maxNodes());
        l.maxComputers(lic.maxComputers());
        l.maxCpus(lic.maxCpus());
        l.maxUpTime(lic.maxUpTime());
        l.gracePeriod(lic.gracePeriod());
        l.attributeName(lic.attributeName());
        l.attributeValue(lic.attributeValue());
        l.disabledSubsystems(lic.disabledSubsystems());
        l.gracePeriodLeft(g.licenseGracePeriodLeft());

        return l;
    }

    /**
     * @return License ID.
     */
    public UUID id() {
        return id;
    }

    /**
     * @param id New license ID.
     */
    public void id(UUID id) {
        this.id = id;
    }

    /**
     * @return License version.
     */
    public String version() {
        return version;
    }

    /**
     * @param ver New license version.
     */
    public void version(String ver) {
        version = ver;
    }

    /**
     * @return GridGain version regular expression.
     */
    public String versionRegexp() {
        return versionRegexp;
    }

    /**
     * @param verRegexp New gridGain version regular expression.
     */
    public void versionRegexp(String verRegexp) {
        versionRegexp = verRegexp;
    }

    /**
     * @return Issue organization.
     */
    public String issueOrganization() {
        return issueOrganization;
    }

    /**
     * @param issueOrganization New issue organization.
     */
    public void issueOrganization(String issueOrganization) {
        this.issueOrganization = issueOrganization;
    }

    /**
     * @return User name.
     */
    public String userName() {
        return userName;
    }

    /**
     * @param userName New user name.
     */
    public void userName(String userName) {
        this.userName = userName;
    }

    /**
     * @return User organization.
     */
    public String userOrganization() {
        return userOrganization;
    }

    /**
     * @param userOrganization New user organization.
     */
    public void userOrganization(String userOrganization) {
        this.userOrganization = userOrganization;
    }

    /**
     * @return User web URL.
     */
    public String userWww() {
        return userWww;
    }

    /**
     * @param userWww New user web URL.
     */
    public void userWww(String userWww) {
        this.userWww = userWww;
    }

    /**
     * @return User email.
     */
    public String userEmail() {
        return userEmail;
    }

    /**
     * @param userEmail New user email.
     */
    public void userEmail(String userEmail) {
        this.userEmail = userEmail;
    }

    /**
     * @return License note.
     */
    public String note() {
        return note;
    }

    /**
     * @param note New license note.
     */
    public void note(String note) {
        this.note = note;
    }

    /**
     * @return Expiration date.
     */
    @Nullable public Date expireDate() {
        return expireDate == null ? null : (Date)expireDate.clone();
    }

    /**
     * @param expireDate New expiration date.
     */
    public void expireDate(@Nullable Date expireDate) {
        this.expireDate = expireDate == null ? null : (Date)expireDate.clone();
    }

    /**
     * @return Issue date.
     */
    public Date issueDate() {
        return (Date)issueDate.clone();
    }

    /**
     * @param issueDate New issue date.
     */
    public void issueDate(Date issueDate) {
        this.issueDate = (Date)issueDate.clone();
    }

    /**
     * @return Maintenance Time.
     */
    public int maintenanceTime() {
        return maintenanceTime;
    }

    /**
     * @param maintenanceTime New maintenance Time.
     */
    public void maintenanceTime(int maintenanceTime) {
        this.maintenanceTime = maintenanceTime;
    }

    /**
     * @return Maximum nodes.
     */
    public int maxNodes() {
        return maxNodes;
    }

    /**
     * @param maxNodes New maximum nodes.
     */
    public void maxNodes(int maxNodes) {
        this.maxNodes = maxNodes;
    }

    /**
     * @return Maximum computers.
     */
    public int maxComputers() {
        return maxComputers;
    }

    /**
     * @param maxComputers New maximum computers.
     */
    public void maxComputers(int maxComputers) {
        this.maxComputers = maxComputers;
    }

    /**
     * @return Maximum CPUs.
     */
    public int maxCpus() {
        return maxCpus;
    }

    /**
     * @param maxCpus New maximum CPUs.
     */
    public void maxCpus(int maxCpus) {
        this.maxCpus = maxCpus;
    }

    /**
     * @return Maximum up-time.
     */
    public long maxUpTime() {
        return maxUpTime;
    }

    /**
     * @param maxUpTime New maximum up-time.
     */
    public void maxUpTime(long maxUpTime) {
        this.maxUpTime = maxUpTime;
    }

    /**
     * @return Maximum grace period in minutes.
     */
    public long gracePeriod() {
        return gracePeriod;
    }

    /**
     * @param gracePeriod New maximum grace period in minutes.
     */
    public void gracePeriod(long gracePeriod) {
        this.gracePeriod = gracePeriod;
    }

    /**
     * @return Attribute name.
     */
    @Nullable public String attributeName() {
        return attributeName;
    }

    /**
     * @param attributeName New attribute name.
     */
    public void attributeName(@Nullable String attributeName) {
        this.attributeName = attributeName;
    }

    /**
     * @return Attribute value.
     */
    @Nullable public String attributeValue() {
        return attributeValue;
    }

    /**
     * @param attributeVal New attribute value.
     */
    public void attributeValue(@Nullable String attributeVal) {
        attributeValue = attributeVal;
    }

    /**
     * @return Disabled sub-systems.
     */
    @Nullable public String disabledSubsystems() {
        return disabledSubsystems;
    }

    /**
     * @param disabledSubsystems New disabled sub-systems.
     */
    public void disabledSubsystems(@Nullable String disabledSubsystems) {
        this.disabledSubsystems = disabledSubsystems;
    }

    /**
     * @return Grace period left in minutes if bursting or {@code -1} otherwise.
     */
    public long gracePeriodLeft() {
        return gracePeriodLeft;
    }

    /**
     * @param gracePeriodLeft New grace period left in minutes if bursting or {@code -1} otherwise.
     */
    public void gracePeriodLeft(long gracePeriodLeft) {
        this.gracePeriodLeft = gracePeriodLeft;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(VisorLicense.class, this);
    }
}
