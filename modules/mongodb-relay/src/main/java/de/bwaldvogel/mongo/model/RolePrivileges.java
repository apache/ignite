package de.bwaldvogel.mongo.model;

import java.util.List;
import java.util.Map;


public class RolePrivileges {
  public static String[] defaultActions = {"find","dbStats","update","insert","remove","count"};
  // role name
  String name;
 
  // {db:db,collection:collection}
  Map<String,String> resource; 
  // find,dbStats,update,insert,remove,createUser
  List<String> actions;
}
