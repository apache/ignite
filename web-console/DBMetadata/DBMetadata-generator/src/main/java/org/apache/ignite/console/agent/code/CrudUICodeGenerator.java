package org.apache.ignite.console.agent.code;

import java.io.ByteArrayOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.zip.ZipOutputStream;

import com.stranger.common.config.GenConfig;
import com.stranger.domain.GenTable;
import com.stranger.domain.GenTableData;
import com.stranger.mapper.impl.GenMapperImpl;
import com.stranger.mapper.impl.GenTableColumnMapperImpl;
import com.stranger.service.GenService;
import com.stranger.service.impl.GenServiceImpl;
import org.apache.commons.io.IOUtils;

public class CrudUICodeGenerator {


	private final GenService genService;

	public CrudUICodeGenerator(){
		genService = new GenServiceImpl(new GenMapperImpl(),new GenTableColumnMapperImpl());
	}

	private void generatorTable(GenConfig config,Map<String,Object> context) throws IOException {
		try {
			List<GenTable> genTables = genService.selectDbTableList(context);
			List<GenTableData> genTableData = genService.buildTableInfo(genTables,config,context);
			ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
			ZipOutputStream zip = new ZipOutputStream(outputStream);
			genService.generatorCode(genTableData, zip);
			IOUtils.closeQuietly(zip);
			FileOutputStream fileOutputStream = new FileOutputStream(config.getFileDownLoadPath());
			fileOutputStream.write(outputStream.toByteArray());
			fileOutputStream.flush();
			fileOutputStream.close();
			System.err.println("<=================代码已经生成=================>");

		} catch (IOException ex) {
			throw ex;
		}
	}
	
	public List<String> generator(String destPath, Map<String,Object> context) {
		List<String> message = new ArrayList<>();
		GenConfig config = new GenConfig();
		String pkgPath = destPath+"org/demo/";
		String domain = "Test";
		
		try {
			config.setPackageName(destPath+"org/demo/");
			config.setFileDownLoadPath(destPath);
			config.setAuthor("demo");
			generatorTable(config,context);
			
		} catch (IOException e) {
			
			e.printStackTrace();
			message.add(e.getMessage());
		}
		
		return message;
	}

}
