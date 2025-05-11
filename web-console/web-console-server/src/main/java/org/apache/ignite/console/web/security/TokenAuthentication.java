package org.apache.ignite.console.web.security;

import org.apache.ignite.console.dto.Account;
import org.springframework.security.authentication.UsernamePasswordAuthenticationToken;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.GrantedAuthority;
import org.springframework.security.core.authority.SimpleGrantedAuthority;

import java.util.ArrayList;
import java.util.Collection;
 
/**
 * Description: spring-security的Authentication的自定义实现（用于校验token）
 */
public class TokenAuthentication implements Authentication{
    private final String token;
    private final Account principal;
    private boolean isAdmin = false;
    
    public TokenAuthentication(String token, Account principal){
        this.token = token;
        this.principal = principal;
        this.isAdmin = principal.isAdmin();
    }
 
    @Override
    public Collection<? extends GrantedAuthority> getAuthorities() {
    	ArrayList<GrantedAuthority> authors = new ArrayList<GrantedAuthority>(1);
    	authors.add(new SimpleGrantedAuthority(Account.ROLE_USER));
    	if(isAdmin) {
    		authors.add(new SimpleGrantedAuthority(Account.ROLE_ADMIN));
    	}
    	return authors;
    }
 
    @Override
    public Object getCredentials(){
        return token;
    }
    
    public TokenAuthentication isAdmin(boolean admin) {
    	this.isAdmin = admin;
    	return this;
    }
 
    @Override
    public Object getDetails() {
        return principal;
    }
 
    @Override
    public Object getPrincipal() {
        return principal;
    }
 
    @Override
    public boolean isAuthenticated() {
        return true;
    }
 
    @Override
    public void setAuthenticated(boolean isAuthenticated) throws IllegalArgumentException {
 
    }
 
    @Override
    public String getName() {
        return principal.toString();
    }
}
