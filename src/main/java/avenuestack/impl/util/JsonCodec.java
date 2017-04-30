package avenuestack.impl.util;

import java.util.HashMap;
import java.util.Map;

import com.fasterxml.jackson.databind.ObjectMapper;

public class JsonCodec {

	static ObjectMapper mapper = new ObjectMapper();

	public static Map<String,Object> parseObject(String s) {
        if( s == null || s.equals("") ) return null;
        try {
            String escapeStr = s.replaceAll("[\\r\\n]"," ");
            Map map = mapper.readValue(escapeStr, Map.class);  
            return map;
        } catch(Exception e) {
            return null;
        }
    }

	public static Map<String,Object> parseObjectNotNull(String s) {
		Map<String,Object> m = parseObject(s);
        if( m == null ) return new HashMap<String,Object>();
        return m;
    }
	
}
