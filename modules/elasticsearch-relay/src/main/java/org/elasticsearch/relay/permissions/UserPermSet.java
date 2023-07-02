package org.elasticsearch.relay.permissions;

import java.util.ArrayList;
import java.util.List;

/**
 * Set of permissions of a user and further information including their liferay
 * ID, liferay groups, liferay roles and nuxeo groups.
 */
public class UserPermSet {
	// TODO: negative number to imply guest?
	private static final String DEF_LR_ID = "0";

	private final String fUserName;

	private final List<String> fLiferayGroups, fLiferayRoles;

	private final List<String> fNuxeoGroups;

	private String fLiferayId;

	/**
	 * @param name
	 *            ID of the user this set is for
	 */
	public UserPermSet(String name) {
		fUserName = name;

		fLiferayGroups = new ArrayList<String>();
		fLiferayRoles = new ArrayList<String>();

		fNuxeoGroups = new ArrayList<String>();

		setLiferayId(DEF_LR_ID);
	}

	/**
	 * @return user ID
	 */
	public String getUserName() {
		return fUserName;
	}

	public List<String> getLiferayGroups() {
		return fLiferayGroups;
	}

	public List<String> getLiferayRoles() {
		return fLiferayRoles;
	}

	public List<String> getNuxeoGroups() {
		return fNuxeoGroups;
	}

	public String getLiferayId() {
		return fLiferayId;
	}

	public void setLiferayId(String liferayId) {
		this.fLiferayId = liferayId;
	}
}
