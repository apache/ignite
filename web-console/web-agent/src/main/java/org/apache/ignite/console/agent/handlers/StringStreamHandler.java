package org.apache.ignite.console.agent.handlers;

import java.io.ByteArrayOutputStream;
import java.io.UnsupportedEncodingException;
import java.util.logging.Formatter;
import java.util.logging.LogRecord;
import java.util.logging.StreamHandler;

public class StringStreamHandler extends StreamHandler{

	static Formatter formatter = new Formatter() {
         @Override public String format(LogRecord record) {
             return record.getMessage() + "\n";
         }
	};
	
	private ByteArrayOutputStream out = new ByteArrayOutputStream();
	
	public StringStreamHandler() {		
		super.setOutputStream(out);
		super.setFormatter(formatter);
		try {
			super.setEncoding("UTF-8");
		} catch (SecurityException | UnsupportedEncodingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public String getOutput() {
		try {
			this.flush();
			return new String(out.toByteArray(),"UTF-8");
		} catch (UnsupportedEncodingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return new String(out.toByteArray());
		}finally {
			out.reset();
		}
	}
	
	public boolean isLoggable(LogRecord record) {
		if (record == null) {
            return false;
        }
		return true;
	}
}
