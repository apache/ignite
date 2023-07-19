package org.apache.ignite.console.web.security;

import javax.servlet.FilterConfig;

import org.apache.commons.lang3.StringUtils;
import org.apache.ignite.console.dto.Account;
import org.apache.ignite.console.services.AccountsService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Configuration;


import org.springframework.context.annotation.Configuration;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.context.SecurityContextHolder;
 
import javax.servlet.*;
import javax.servlet.http.HttpServletRequest;
import java.io.IOException;


/**
 * Description: 用于处理收到的token并为spring-security上下文生成及注入Authenticaion实例
 */
public class AuthenticationTokenFilter implements Filter{
	
	
	private AccountsService accountsService;
	
	@Override
    public void init(FilterConfig filterConfig) throws ServletException{
 
    }
 
    @Override
    public void doFilter(ServletRequest servletRequest, ServletResponse servletResponse,FilterChain filterChain)
            throws IOException, ServletException{
        if (servletRequest instanceof HttpServletRequest){
            String token = ((HttpServletRequest) servletRequest).getHeader("TOKEN");
            if (!StringUtils.isEmpty(token)){
            	Account account = accountsService.getAccountByToken(token);
            	if(account!=null) {
            		TokenAuthentication authentication = new TokenAuthentication(token,account);
	                SecurityContextHolder.getContext().setAuthentication(authentication);
	                System.out.println("Set authentication with non-empty token");
            	}
            	else {
            		
            	}
            	
            } else {
               
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
