package org.apache.ignite.console.agent.code;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.ignite.console.agent.db.CrudTemplate;

public class CrudUICodeGenerator {
	
	public List<String> generator(String destPath, Map<String,Object> context) {
		List<String> message = new ArrayList<>();
		String pkgPath = destPath+"org/demo/";
		String domain = "Test";
		
		CrudTemplate crudT = new CrudTemplate();
		
		try {
			crudT.exportRepository(context, pkgPath, domain);
			
			crudT.exportCrudView(context, pkgPath, domain);
			
		} catch (IOException e) {
			
			e.printStackTrace();
			message.add(e.getMessage());
		}
		
		return message;
	}

}
