

package org.apache.ignite.console.dto;

import java.time.LocalDateTime;
import java.util.Collection;
import java.util.UUID;
import org.apache.ignite.console.notification.IRecipient;
import org.apache.ignite.console.web.model.ChangeUserRequest;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.springframework.security.core.CredentialsContainer;
import org.springframework.security.core.GrantedAuthority;
import org.springframework.security.core.userdetails.UserDetails;

import static org.springframework.security.core.authority.AuthorityUtils.createAuthorityList;

/**
 * DTO for Account.
 */
public class Account extends AbstractDto implements UserDetails, CredentialsContainer, IRecipient {
    /** */
    public static final String ROLE_USER = "ROLE_USER";

    /** */
    public static final String ROLE_ADMIN = "ROLE_ADMIN";

    /** Email. */
    private String email;

    /** Salt + hash for password. */
    private String hashedPwd;

    /** First name. */
    private String firstName;

    /** Last name. */
    private String lastName;

    /** Phone. */
    private String phone;

    /** Company. */
    private String company;

    /** Country. */
    private String country;

    /** Agent token. */
    private String tok;

    /** Reset password token. */
    private String resetPwdTok;

    /** Last login. */
    private String lastLogin;

    /** Last activity. */
    private String lastActivity;
    
    /** User create index */
    private int uid; 

    /** Administration. */
    private boolean admin;

    /** Indicates whether the user is enabled or disabled. */
    private boolean enabled = true;

    /** Latest activation token. */
    private UUID activationTok;

    /** Latest activation token was sent at. */
    private LocalDateTime activationSentAt;

    /**
     * Default constructor for serialization.
     */
    public Account() {
        // No-op.
    }

    /**
     * Full constructor.
     *
     * @param email Email.
     * @param hashedPwd salt + hash for password.
     * @param firstName First name.
     * @param lastName Last name.
     * @param phone Phone.
     * @param company Company name.
     * @param country Country name.
     */
    public Account(
        String email,
        String hashedPwd,
        String firstName,
        String lastName,
        String phone,
        String company,
        String country
    ) {
        super(UUID.randomUUID());

        this.email = email;
        this.hashedPwd = hashedPwd;
        this.firstName = firstName;
        this.lastName = lastName;
        this.phone = phone;
        this.company = company;
        this.country = country;

        this.tok = UUID.randomUUID().toString();
        this.resetPwdTok = UUID.randomUUID().toString();
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
     * @return e-mail.
     */
    @Override public String getEmail() {
        return email;
    }

    /**
     * @param email New email.
     */
    public void setEmail(String email) {
        this.email = email;
    }

    /**
     * @return Phone.
     */
    @Override public String getPhone() {
        return phone;
    }

    /**
     * @param phone New phone.
     */
    public void setPhone(String phone) {
        this.phone = phone;
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
     * @return Token.
     */
    public String getToken() {
        return tok;
    }

    /**
     * @param tok New token.
     */
    public void setToken(String tok) {
        this.tok = tok;
    }

    /**
     * @return Admin flag.
     */
    public boolean isAdmin() {
        return admin;
    }

    /**
     * @param admin Admin flag.
     */
    public void setAdmin(boolean admin) {
        this.admin = admin;
    }

    /**
     * @return Last login.
     */
    public String lastLogin() {
        return lastLogin;
    }

    /**
     * @return Last activity.
     */
    public String lastActivity() {
        return lastActivity;
    }

    /**
     * @return Reset password token.
     */
    public String getResetPasswordToken() {
        return resetPwdTok;
    }

    /**
     * @param resetPwdTok Reset password token.
     */
    public void setResetPasswordToken(String resetPwdTok) {
        this.resetPwdTok = resetPwdTok;
    }

    /**
     * @return Latest activation token.
     */
    public UUID getActivationToken() {
        return activationTok;
    }

    /**
     * @return Latest activation token was sent at.
     */
    public LocalDateTime getActivationSentAt() {
        return activationSentAt;
    }

    /**
     * Activate account.
     */
    public void activate() {
        enabled = true;
        activationTok = null;
        activationSentAt = null;
    }

    /**
     * Reset activation token.
     */
    public void resetActivationToken() {
        enabled = false;
        activationTok = UUID.randomUUID();
        activationSentAt = LocalDateTime.now();
    }

    /** {@inheritDoc} */
    @Override public Collection<? extends GrantedAuthority> getAuthorities() {
        return admin
            ? createAuthorityList(ROLE_USER, ROLE_ADMIN)
            : createAuthorityList(ROLE_USER);
    }

    /** {@inheritDoc} */
    @Override public String getUsername() {
        return email;
    }

    /** {@inheritDoc} */
    @Override public String getPassword() {
        return hashedPwd;
    }

    /**
     * @param hashedPwd New hashed password.
     */
    public void setPassword(String hashedPwd) {
        this.hashedPwd = hashedPwd;
    }

    /** {@inheritDoc} */
    @Override public boolean isAccountNonExpired() {
        return true;
    }

    /** {@inheritDoc} */
    @Override public boolean isAccountNonLocked() {
        return true;
    }

    /** {@inheritDoc} */
    @Override public boolean isCredentialsNonExpired() {
        return true;
    }

    /** {@inheritDoc} */
    @Override public boolean isEnabled() {
        return enabled;
    }

    /** {@inheritDoc} */
    @Override public void eraseCredentials() {
        hashedPwd = null;
    }   
    

    public int getUid() {
		return uid;
	}

	public void setUid(int uid) {
		this.uid = uid;
	}

	/**
     * Update account fields.
     * @param changes Changes.
     */
    public void update(ChangeUserRequest changes) {
        email = changes.getEmail();
        firstName = changes.getFirstName();
        lastName = changes.getLastName();
        phone = changes.getPhone();
        country = changes.getCountry();
        company = changes.getCompany();
        tok = changes.getToken();
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(Account.class, this);
    }
}
