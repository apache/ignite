

package org.apache.ignite.console.web.model;

import io.swagger.v3.oas.annotations.media.Schema;

import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.NotNull;
import org.apache.ignite.console.dto.Account;
import org.apache.ignite.internal.util.typedef.internal.S;


/**
 * Base class for user web models.
 */
public class User {
	/** Email. */
    @Schema(title = "User uid")    
    private String uid;
    
    /** Email. */
    @Schema(title = "User email", required = true)
    @NotNull
    @NotEmpty
    private String email;

    /** First name. */
    @Schema(title = "User first name", required = true)
    @NotNull
    @NotEmpty
    private String firstName;

    /** Last name. */
    @Schema(title = "User last name", required = true)
    @NotNull
    @NotEmpty
    private String lastName;
    
    /** display name. */
    @Schema(title = "User display name")    
    private String displayName;


    /** Phone. */
    @Schema(title = "User phone")
    private String phone;    
    
    /** photo url */
    @Schema(title = "User photo URL")    
    private String photoURL;

    /** Company. */
    @Schema(title = "User company", required = true)
    @NotNull
    @NotEmpty
    private String company;

    /** Country. */
    @Schema(title = "User country", required = true)
    @NotNull
    @NotEmpty
    private String country;

    /**
     * Default constructor for serialization.
     */
    public User() {
        // No-op.
    }

    /**
     * Full constructor.
     *
     * @param acc AccountDTO.
     */
    public User(Account acc) {
    	uid = acc.getId().toString();
        email = acc.getEmail();
        firstName = acc.getFirstName();
        lastName = acc.getLastName();
        phone = acc.getPhone();
        company = acc.getCompany();
        country = acc.getCountry();
    }

    /**
     * @return Email.
     */
    public String getEmail() {
        return email;
    }

    /**
     * @param email New email.
     */
    public void setEmail(String email) {
        this.email = email;
    }

    /**
     * @return First name.
     */
    public String getFirstName() {
        return firstName;
    }

    /**
     * @param firstName New first name.
     */
    public void setFirstName(String firstName) {
        this.firstName = firstName;
    }

    /**
     * @return Last name.
     */
    public String getLastName() {
        return lastName;
    }

    /**
     * @param lastName New last name.
     */
    public void setLastName(String lastName) {
        this.lastName = lastName;
    }

    /**
     * @return Company.
     */
    public String getCompany() {
        return company;
    }

    /**
     * @param company New company.
     */
    public void setCompany(String company) {
        this.company = company;
    }

    /**
     * @return Country.
     */
    public String getCountry() {
        return country;
    }

    /**
     * @param country New country.
     */
    public void setCountry(String country) {
        this.country = country;
    }

    /**
     * @return Phone.
     */
    public String getPhone() {
        return phone;
    }

    /**
     * @param phone New phone.
     */
    public void setPhone(String phone) {
        this.phone = phone;
    }

    public String getUid() {
		return uid;
	}

	public void setUid(String uid) {
		this.uid = uid;
	}

	public String getDisplayName() {
		return displayName;
	}

	public void setDisplayName(String displayName) {
		this.displayName = displayName;
	}

	public String getPhotoURL() {
		return photoURL;
	}

	public void setPhotoURL(String photoURL) {
		this.photoURL = photoURL;
	}

	/** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(User.class, this);
    }
}
