package de.bwaldvogel.mongo.model;

import java.util.List;
import java.util.Map;
import java.util.UUID;

public class UserInfo {
	//"_id" : "<db>.<username>",
	String _id;	
    //"userId" : <UUID>,        // Starting in MongoDB 4.0.9
	UUID userId;
    //"user" : "<username>",
	String user;
    //"db" : "<db>",
    String db;    
    
    //"customData" : <document>,
	Map<String,Object> customData;
	
    //"roles" : [ ... ],
	List<String> roles;
	
    //"credentials": { ... }, // only if showCredentials: true
    //"inheritedRoles" : [ ... ],  // only if showPrivileges: true or showAuthenticationRestrictions: true
    //"inheritedPrivileges" : [ ... ], // only if showPrivileges: true or showAuthenticationRestrictions: true
    //"inheritedAuthenticationRestrictions" : [ ] // only if showPrivileges: true or showAuthenticationRestrictions: true
    //"authenticationRestrictions" : [ ... ] 
}
