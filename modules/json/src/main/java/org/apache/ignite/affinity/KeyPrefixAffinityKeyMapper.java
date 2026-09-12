package org.apache.ignite.affinity;

import org.apache.ignite.internal.processors.cache.GridCacheDefaultAffinityKeyMapper;

/**
 *  只使用key的前面冒号开头的字符串前缀
 * @author admin
 *
 */
public class KeyPrefixAffinityKeyMapper extends GridCacheDefaultAffinityKeyMapper{		
	private static final long serialVersionUID = 1L;
	private char splitChar;

	public KeyPrefixAffinityKeyMapper(){
		this(':');
	}
	
	public KeyPrefixAffinityKeyMapper(char splitChar){
		this.splitChar = splitChar;
	}
	
	public char getSplitChar() {
		return splitChar;
	}

	public void setSplitChar(char splitChar) {
		this.splitChar = splitChar;
	}

	@Override public Object affinityKey(Object key) {
		 if(key instanceof String) {
			 String strKey = key.toString();
			 int pos = strKey.indexOf(splitChar);
			 if(pos>0) {
				 return strKey.substring(0,pos);
			 }
			 return key;
		 }
		 return super.affinityKey(key);
	 }
			
}
