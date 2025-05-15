package org.apache.ignite.console.agent.db;

import java.io.File;
import java.io.IOException;
import java.util.Map;

import org.beetl.core.Template;

public class CrudTemplate extends BeetlTemplate{
	
	public String exportRepository(Map<String,Object> context,String filePath, String fileName) throws IOException{
		mkdirs(filePath);
        Template t = gt.getTemplate("repository.java.tpl");
        t.binding(context);
        t.binding("fileName", fileName);
        String path = filePath + File.separator + fileName + "Repository.java";
        render(t, path);
        return path;
	}
	
	public String exportCrudView(Map<String,Object> context,String filePath, String fileName) throws IOException{
		mkdirs(filePath);
        Template t = gt.getTemplate("crudview.java.tpl");
        t.binding(context);
        t.binding("fileName", fileName);
        String path = filePath + File.separator + fileName + "CrudView.java";
        render(t, path);
        return path;
	}
	

}
