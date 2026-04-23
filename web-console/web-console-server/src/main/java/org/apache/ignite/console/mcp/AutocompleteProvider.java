package org.apache.ignite.console.mcp;


import org.apache.ignite.console.services.AccountsService;
import org.springaicommunity.mcp.annotation.McpComplete;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.stream.Collectors;

@Service
public class AutocompleteProvider {

    @Autowired
    AccountsService accountsService;

    @McpComplete(uri = "user:{username}")
    public List<String> completeUsername(final String usernamePrefix) {
        // Implementation to provide username completions
        return accountsService.list().stream().filter(a->a.getUsername().startsWith(usernamePrefix)).map(a->a.getUsername()).collect(Collectors.toList());
    }

    // Additional completion methods...
}
