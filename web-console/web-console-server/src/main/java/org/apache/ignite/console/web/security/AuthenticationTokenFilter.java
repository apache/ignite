package org.apache.ignite.console.web.security;

import javax.servlet.FilterConfig;

import org.apache.commons.lang3.StringUtils;
import org.apache.ignite.console.dto.Account;
import org.apache.ignite.console.services.AccountsService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Configuration;


import org.springframework.context.annotation.Configuration;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.context.SecurityContextHolder;
 
import javax.servlet.*;
import javax.servlet.http.HttpServletRequest;
import java.io.IOException;
import java.util.Enumeration;


/**
 * Description: 用于处理收到的token并为spring-security上下文生成及注入Authenticaion实例
 */
public class AuthenticationTokenFilter implements Filter{
	
	private static final Logger log = LoggerFactory.getLogger(AuthenticationTokenFilter.class);
	private AccountsService accountsService;
	
	@Override
    public void init(FilterConfig filterConfig) throws ServletException{
 
    }
 
    @Override
    public void doFilter(ServletRequest servletRequest, ServletResponse servletResponse,FilterChain filterChain)
            throws IOException, ServletException{
        if (servletRequest instanceof HttpServletRequest){
        	String uri = ((HttpServletRequest) servletRequest).getRequestURI();
        	System.out.println(uri);
            String authorization = ((HttpServletRequest) servletRequest).getHeader("Authorization");
            if (!StringUtils.isEmpty(authorization) && authorization.toLowerCase().startsWith("token ")){
            	String token = authorization.substring(6);
            	Account account = accountsService.getAccountByToken(token);
            	if(account!=null) {
            		TokenAuthentication authentication = new TokenAuthentication(token,account);
	                SecurityContextHolder.getContext().setAuthentication(authentication);
	                log.info("Set authentication with non-empty token");
            	}
            	else {
            		log.warn("Can not found user for token "+ token);
            	}
            	
            }
        }
        filterChain.doFilter(servletRequest, servletResponse);
    }

	public AccountsService getAccountsService() {
		return accountsService;
	}

	public void setAccountsService(AccountsService accountsService) {
		this.accountsService = accountsService;
	}

}
