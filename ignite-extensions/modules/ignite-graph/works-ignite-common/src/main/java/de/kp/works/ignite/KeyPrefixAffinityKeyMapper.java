package de.kp.works.ignite;

import org.apache.ignite.internal.processors.cache.GridCacheDefaultAffinityKeyMapper;

/**
 *  只使用key的前面冒号开头的字符串前缀
 * @author admin
 *
 */
public class KeyPrefixAffinityKeyMapper extends GridCacheDefaultAffinityKeyMapper{		
	private static final long serialVersionUID = 1L;

	@Override public Object affinityKey(Object key) {
		 if(key instanceof String) {
			 String strKey = key.toString();
			 int pos = strKey.indexOf(':');
			 if(pos>0) {
				 return strKey.substring(0,pos);
			 }
			 return key;
		 }
		 return super.affinityKey(key);
	 }
			
}
