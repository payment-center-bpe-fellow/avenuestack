package avenuestack.impl.avenue;

import java.util.HashMap;

class XheadType {
	int code;
	String name;
	String cls;

	XheadType(int code, String name, String cls) {
		this.code = code;
		this.name = name;
		this.cls = cls;
	}
}

public class Xhead {

    public final static String KEY_SOC_ID = "socId";
    public final static String KEY_ADDRS = "addrs";
    public final static String KEY_UNIQUE_ID = "uniqueId";
    public final static String KEY_SPS_ID = "spsId";
    public final static String KEY_HTTP_TYPE = "httpType";
    public final static String KEY_BUSINESS_TYPE = "businessType";

    public final static int CODE_SOC_ID = 1;
    public final static int CODE_ADDRS = 2;
    public final static int CODE_UNIQUE_ID = 9;
    public final static int CODE_SPS_ID = 11;
    public final static int CODE_HTTP_TYPE = 12;

    public final static int CODE_BUSINESS_TYPE = 16;

    public final static String KEY_FIRST_ADDR = "firstAddr";
    public final static String KEY_LAST_ADDR = "lastAddr";

    static HashMap<Integer, XheadType> codeMap = new HashMap<Integer, XheadType>();
    static HashMap<String, XheadType> nameMap = new HashMap<String, XheadType>();

    static {
    	init();
    }

    static void init() {
        add(0, "signature", "bytes");
        add(CODE_SOC_ID, KEY_SOC_ID, "string");
        add(CODE_ADDRS, KEY_ADDRS, "addr");
        add(3, "appId", "int");
        add(4, "areaId", "int");
        add(5, "groupId", "int");
        add(6, "hostId", "int");
        add(7, "spId", "int");
        add(8, "endpointId", "string");
        add(CODE_UNIQUE_ID, KEY_UNIQUE_ID, "string"); // 9
        add(CODE_SPS_ID, KEY_SPS_ID, "string"); // 11 
        add(CODE_HTTP_TYPE, KEY_HTTP_TYPE, "int"); // 12
        add(13, "logId", "string");
        add(CODE_BUSINESS_TYPE, KEY_BUSINESS_TYPE, "string");//16
    }

    static void add(int code,String name,String tp) {
    	XheadType v = new XheadType(code, name, tp);
        codeMap.put(code, v);
        nameMap.put(name, v);
    }
}
