package avenuestack.impl.avenue;

import static avenuestack.impl.util.ArrayHelper.mkString;

import java.io.FileInputStream;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.dom4j.Element;
import org.dom4j.io.SAXReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import avenuestack.ErrorCodes;
import avenuestack.impl.util.ArrayHelper;
import avenuestack.impl.util.TypeSafe;

public class TlvCodec {

	static Logger log = LoggerFactory.getLogger(TlvCodec.class);

	static String CLASSPATH_PREFIX = "classpath:";
	
	static final int CLS_ARRAY = -2;
	static final int CLS_UNKNOWN = -1;

	static final int CLS_STRING = 1;
	static final int CLS_INT = 2;
	static final int CLS_STRUCT = 3;

	static final int CLS_STRINGARRAY = 4;
	static final int CLS_INTARRAY = 5;
	static final int CLS_STRUCTARRAY = 6;

	static final int CLS_BYTES = 7;

	static final int CLS_SYSTEMSTRING = 8; // used only in struct
	
	static final String[] EMPTY_STRINGARRAY = new String[0];
	static final HashMap<String,String> EMPTY_STRINGMAP = new HashMap<String,String>();
	
    static int MASK = 0xFF;
    static char[] HEX_CHARS = new char[]{'0', '1', '2', '3', '4', '5', '6','7', '8', '9', 'a', 'b', 'c', 'd', 'e', 'f'};

    static boolean isArray(int cls) { return cls == CLS_STRINGARRAY || cls == CLS_INTARRAY || cls == CLS_STRUCTARRAY; }

    static int clsToArrayType(int cls) {
        switch(cls) {
            case CLS_STRING:
            	return CLS_STRINGARRAY;
            case CLS_INT:
            	return CLS_INTARRAY;
            case CLS_STRUCT:
            	return CLS_STRUCTARRAY;
            default: 
            	return CLS_UNKNOWN;
        }
    }

    static int clsToInt(String cls) {
    	return clsToInt(cls,"");
    }
    
    static int clsToInt(String cls,String isbytes) {
        String lcls = cls.toLowerCase();
        if(lcls.equals("string")) {
        	if( isbytes.equals("1") || isbytes.equals("true")  || isbytes.equals("TRUE") || isbytes.equals("y") || isbytes.equals("YES") )
        		return CLS_BYTES; 
        	else 
        		return CLS_STRING;
        }
        if(lcls.equals("int")) {
        	return CLS_INT;
        }
        if(lcls.equals("struct")) {
        	return CLS_STRUCT;
        }
        if(lcls.equals("array")) {
        	return CLS_ARRAY;
        }
        if(lcls.equals("systemstring")) {
        	return CLS_SYSTEMSTRING;
        }
        return CLS_UNKNOWN;
    }

    static String bytes2hex(byte[] b) {
        StringBuilder ss = new StringBuilder();

        for ( int i = 0; i< b.length; ++i ) {
            int t = b[i] & MASK;
            int hi = t / 16;
            int lo = t % 16;
            ss.append( HEX_CHARS[hi] );
            ss.append( HEX_CHARS[lo] );
            ss.append(" ");
        }

        return ss.toString().trim();
    }

    static String toHexString(ByteBuffer buff) {
        return bytes2hex(buff.array());
    }
    
	String configFile;

    int bufferSize = 1000;
    ByteBuffer EMPTY_BUFFER = ByteBuffer.allocate(0);

    public HashMap<Integer,TlvType> typeCodeToNameMap = new HashMap<Integer,TlvType>();
    public HashMap<String,TlvType> typeNameToCodeMap = new HashMap<String,TlvType>();

    public HashMap<Integer,String> msgIdToNameMap = new HashMap<Integer,String>();
    public HashMap<String,Integer> msgNameToIdMap = new HashMap<String,Integer>();
    public HashMap<Integer,String> msgIdToOrigNameMap = new HashMap<Integer,String>();

    public HashMap<Integer,HashMap<String,String>> msgKeyToTypeMapForReq = new HashMap<Integer,HashMap<String,String>>();
    public HashMap<Integer,HashMap<String,String>> msgTypeToKeyMapForReq = new HashMap<Integer,HashMap<String,String>>();
    public HashMap<Integer,ArrayList<String>> msgKeysForReq = new HashMap<Integer,ArrayList<String>>();
    public HashMap<Integer,HashMap<String,TlvFieldInfo>> msgKeyToFieldInfoMapForReq = new HashMap<Integer,HashMap<String,TlvFieldInfo>>();

    public HashMap<Integer,HashMap<String,String>> msgKeyToTypeMapForRes = new HashMap<Integer,HashMap<String,String>>();
    public HashMap<Integer,HashMap<String,String>> msgTypeToKeyMapForRes = new HashMap<Integer,HashMap<String,String>>();
    public HashMap<Integer,ArrayList<String>> msgKeysForRes = new HashMap<Integer,ArrayList<String>>();
    public HashMap<Integer,HashMap<String,TlvFieldInfo>> msgKeyToFieldInfoMapForRes = new HashMap<Integer,HashMap<String,TlvFieldInfo>>();

    public HashMap<String,String> codecAttributes = new HashMap<String,String>();

    public HashMap<Integer,HashMap<String,String>> msgAttributes = new HashMap<Integer,HashMap<String,String>>();

    public int serviceId;
    public String serviceName;
    public String serviceOrigName;
    public boolean enableExtendTlv = true;

    TlvCodec(String configFile) {
    	this.configFile = configFile;
    	init();
    }
    
    void init() {
        try {
            initInternal();
        }catch(Exception ex) {
            log.error("tlv init failed, file={}, error={}",configFile,ex.getMessage());
            throw new CodecException(ex.getMessage());
        }
    }

    public ArrayList<TlvType> allTlvTypes() {
    	ArrayList<TlvType> list = new ArrayList<TlvType>();
    	for(TlvType i:typeNameToCodeMap.values())
    		list.add(i);
        return list;
    }
    public ArrayList<Integer> allMsgIds() {
    	ArrayList<Integer> list = new ArrayList<Integer>();
    	for(Integer i: msgKeyToTypeMapForReq.keySet())
    		list.add(i);
        return list;
    }
    TlvType findTlvType(int code) {
        return typeCodeToNameMap.get(code);
    }

    String getAttribute(Element t,String field1) {
    	String s = t.attributeValue(field1,"");
    	if( s == null ) s = "";
    	return s;
    }

    boolean isEmpty(String s) {
    	return s == null || s.length() == 0;
    }
    
    TlvFieldInfo getTlvFieldInfo(Element t) {
        // 严格区分 null 和 空串
        String defaultValue = t.attributeValue("default");

        String validatorCls  = getAttribute(t,"validator");
        if( isEmpty(validatorCls) ) validatorCls = null;
		String validatorParam  = getAttribute(t,"validatorParam");
		String returnCode  = getAttribute(t,"returnCode");
		String encoderCls  = getAttribute(t,"encoder");
		if( isEmpty(encoderCls) ) encoderCls = null;
		String encoderParam  = getAttribute(t,"encoderParam");
        return TlvFieldInfo.getTlvFieldInfo(defaultValue,validatorCls,validatorParam,returnCode,encoderCls,encoderParam);
    }

    TlvFieldInfo getTlvFieldInfo(Element t,TlvType tlvType) {

    	TlvFieldInfo fieldInfo = getTlvFieldInfo(t);
        if( fieldInfo == null ) return tlvType.fieldInfo;
        if( tlvType.fieldInfo == null ) return fieldInfo;

        String defaultValue =  fieldInfo.defaultValue != null ? fieldInfo.defaultValue : tlvType.fieldInfo.defaultValue;

        String validatorCls = null;
		String validatorParam  = "";
		String returnCode = "";

        if( fieldInfo.validator != null ) {
            validatorCls  = fieldInfo.validatorCls;
            validatorParam  = fieldInfo.validatorParam;
            returnCode  = fieldInfo.validatorReturnCode;
        } else if( tlvType.fieldInfo.validator != null ) {
            validatorCls  = tlvType.fieldInfo.validatorCls;
            validatorParam  = tlvType.fieldInfo.validatorParam;
            returnCode  = tlvType.fieldInfo.validatorReturnCode;
        }

        String encoderCls = null;
		String encoderParam  = "";

        if( fieldInfo.encoder != null ) {
            encoderCls  = fieldInfo.encoderCls;
            encoderParam  = fieldInfo.encoderParam;
        } else if( tlvType.fieldInfo.encoder != null ) {
            encoderCls  = tlvType.fieldInfo.encoderCls;
            encoderParam  = tlvType.fieldInfo.encoderParam;
        }

        return TlvFieldInfo.getTlvFieldInfo(defaultValue,validatorCls,validatorParam,returnCode,encoderCls,encoderParam);
    }

    
    ObjectWithReturnCode validateAndEncodeObject(Object a,TlvFieldInfo fieldInfo,boolean needEncode) {
        if( fieldInfo == null ) return new ObjectWithReturnCode(a,0);
        int errorCode = 0;
        Object b = a;
        if( fieldInfo.validator != null) {
            int ret = fieldInfo.validator.validate(a);
            if( ret != 0 ) {
                log.error("validate_value_error, value="+a+", ret="+ret);
                if( errorCode == 0 ) errorCode = ret;
            }
        }
        if( needEncode && fieldInfo.encoder != null ) {
            b = fieldInfo.encoder.encode(a);
        }
        return new ObjectWithReturnCode(b,errorCode);
    }

    int validateAndEncodeMap(Map<String,Object> map,TlvType tlvType,boolean needEncode) {

        int errorCode = 0;
        int i = 0 ;
        while( i < tlvType.structNames.length ) {
        	TlvFieldInfo fieldInfo = tlvType.structFieldInfos[i];
            if( fieldInfo != null ) {
                String key = tlvType.structNames[i];
                Object value = map.get(key);
                if( fieldInfo.validator != null ) {
                    int ret = fieldInfo.validator.validate(value);
                    if( ret != 0 ) {
                        log.error("validate_map_error, key="+key+", value="+value+", ret="+ret);
                        if( errorCode == 0 ) errorCode = ret;
                    }
                }
                if(  needEncode && fieldInfo.encoder != null ) {
                    Object v = fieldInfo.encoder.encode(value);
                    map.put(key,v);
                }
            }
            i += 1;
        }
        return errorCode;
    }
    
    void setStructDefault(Map<String,Object> map,TlvType tlvType) {

        int i = 0; 
        while( i < tlvType.structNames.length ) {
            TlvFieldInfo fieldInfo = tlvType.structFieldInfos[i];
            if( fieldInfo != null ) {
                String key = tlvType.structNames[i];
                Object value = map.get(key);
                if( value == null && fieldInfo.defaultValue != null ) {
                   map.put(key,fieldInfo.defaultValue);
                }
            }
            i += 1;
        }
    }
    
    int validateAndEncode(HashMap<String,String> keyMap,HashMap<String,TlvFieldInfo> fieldInfoMap,
    		Map<String,Object> map,boolean needEncode) {

        if( fieldInfoMap == null ) return 0;

        int errorCode = 0;
        
        for( Map.Entry<String,TlvFieldInfo> entry : fieldInfoMap.entrySet() ) {
        	String key = entry.getKey();
        	TlvFieldInfo fieldInfo = entry.getValue();
            String tp = keyMap.get(key);
            TlvType tlvType = typeNameToCodeMap.get(tp);
            Object value = map.get(key);
            if( value == null && fieldInfo != null && fieldInfo.defaultValue != null ) {
                map.put(key,fieldInfo.defaultValue);
            } else if( value != null && tlvType.cls == TlvCodec.CLS_STRUCT && value instanceof Map ) {
            	Map<String,Object> m = (Map<String,Object>)value;
                setStructDefault(m,tlvType);
            } else if( value != null && tlvType.cls == TlvCodec.CLS_STRUCTARRAY  && value instanceof List ) {
            	List<Map<String,Object>> lm = (List<Map<String,Object>>)value;
                for( Map<String,Object> m : lm )
                    setStructDefault(m,tlvType);
            }
            value = map.get(key);
            if( value != null ) {
                switch (tlvType.cls) {
                    case TlvCodec.CLS_INT :
                        map.put(key,anyToInt(value));
                        break;
                    case TlvCodec.CLS_STRING :
                        map.put(key,anyToString(value));
                        break;
                    case TlvCodec.CLS_STRUCT :
                        map.put(key,anyToStruct(tlvType,value));
                        break;
                    case TlvCodec.CLS_INTARRAY :
                        map.put(key,anyToIntArray(value));
                        break;
                    case TlvCodec.CLS_STRINGARRAY :
                        map.put(key,anyToStringArray(value));
                        break;
                    case TlvCodec.CLS_STRUCTARRAY :
                        map.put(key,anyToStructArray(tlvType,value));
                        break;
                    default : 
                }
            }

            value = map.get(key);
            if( value == null ) {
                ObjectWithReturnCode ret = validateAndEncodeObject(null,fieldInfo,needEncode);
                if( ret.ec != 0 && errorCode == 0 )  errorCode = ret.ec;
            } else {

                if( value instanceof Integer ) {
                	Integer i = (Integer)value; 
                	ObjectWithReturnCode ret = validateAndEncodeObject(i,fieldInfo,needEncode);
                    if( ret.ec != 0 && errorCode == 0 )  errorCode = ret.ec;
                    map.put(key,ret.v);
                } else if( value instanceof String ) {
                	String s = (String)value; 
                	ObjectWithReturnCode ret = validateAndEncodeObject(s,fieldInfo,needEncode);
                    if( ret.ec != 0 && errorCode == 0 )  errorCode = ret.ec;
                    map.put(key,ret.v);
                } else if( value instanceof Map ) {
                	Map<String,Object> m = (Map<String,Object>)value; 
                	int ec = validateAndEncodeMap(m,tlvType,needEncode);
                    if( ec != 0 && errorCode == 0 )  errorCode = ec;
                } else if( value instanceof List ) {
                	List<Object> l = (List<Object>)value;
                	ObjectWithReturnCode ret = validateAndEncodeObject(l,fieldInfo,needEncode);
                    if( ret.ec != 0 && errorCode == 0 )  errorCode = ret.ec;
                    for(int i = 0 ; i<l.size(); ++i ) {
                    	Object a  = l.get(i);
                    	if( a instanceof Map ) {
                    		Map<String,Object> m = (Map<String,Object>)a; 
                        	int ret2 = validateAndEncodeMap(m,tlvType.itemType,needEncode);
                            if( ret2 != 0 && errorCode == 0 )  errorCode = ret2;
                    	} else {
                        	ret = validateAndEncodeObject(a,tlvType.itemType.fieldInfo,needEncode);
                            if( ret.ec != 0 && errorCode == 0 )  errorCode = ret.ec;
                            l.set(i,ret.v);
                    	}
                    }
                }
            }
        }

        return errorCode;
    }

    boolean isTrue(String s) {
    	return s != null && s.equals("1") || s.equals("y") || s.equals("t") || s.equals("yes") || s.equals("true");
    }
    
	TlvFieldInfo[] toTlvFieldInfoArray(ArrayList<TlvFieldInfo> list) {
		TlvFieldInfo[] b = new TlvFieldInfo[list.size()];
		for(int i=0;i<list.size();++i) {
			b[i] = list.get(i);
		}
		return b;
	}	
	
    void initInternal() throws Exception {

		SAXReader saxReader = new SAXReader();
		saxReader.setEncoding("UTF-8");
		InputStream in;
		if( configFile.startsWith(CLASSPATH_PREFIX))
			in = TlvCodec.class.getResourceAsStream(configFile.substring(CLASSPATH_PREFIX.length()));
		else
			in = new FileInputStream(configFile);
		Element cfgXml = saxReader.read(in).getRootElement();
		in.close();
		
        serviceId = Integer.parseInt(cfgXml.attributeValue("id"));
        serviceOrigName = cfgXml.attributeValue("name");
        serviceName = cfgXml.attributeValue("name").toLowerCase();
        String enableExtendTlvStr = cfgXml.attributeValue("enableExtendTlv","").toLowerCase();
        if( enableExtendTlvStr != null && enableExtendTlvStr.length() > 0 )
        	enableExtendTlv = isTrue(enableExtendTlvStr);
        //val metadatas = cfgXml.attributes.filter(  _.key != "id").filter( _.key != "name").filter( _.key != "IsTreeStruct").filter( _.key != "enableExtendTlv")
        //for( m <- metadatas ) {
          //  codecAttributes.put(m.key,m.value.text)
        //}

        //val codecSql = (cfgXml \ "sql").text.trim
        //if( codecSql != "")
          //  codecAttributes.put("sql",codecSql)

        // if( codecAttributes.size > 0 ) println( codecAttributes )

        List<Element> types = cfgXml.selectNodes("/service/type");
        
		for (Element t : types) {
            String name = t.attributeValue("name").toLowerCase();
            int cls = TlvCodec.clsToInt( t.attributeValue("class"),t.attributeValue("isbytes","") );
            if( cls == TlvCodec.CLS_UNKNOWN || cls == TlvCodec.CLS_SYSTEMSTRING)
                throw new CodecException("class not valid, class="+ t.attributeValue("class"));
            int code =  cls != TlvCodec.CLS_ARRAY ?  Integer.parseInt(t.attributeValue("code")) : 0;

            /*
            val metadatas = t.attributes.filter(  _.key != "code").filter( _.key != "name").filter( _.key != "class").filter( _.key != "isbytes")
            for( m <- metadatas ) {
                codecAttributes.put("type-"+name+"-"+m.key,m.value.text)
            }
            */
            TlvFieldInfo fieldInfo = getTlvFieldInfo(t);

            switch(cls) {

                case TlvCodec.CLS_ARRAY:
                {	
                    String itemType = t.attributeValue("itemType","").toLowerCase();
                    TlvType itemConfig = typeNameToCodeMap.get(itemType);
                    if(itemConfig == null ) itemConfig = TlvType.UNKNOWN;
                    if( itemConfig == TlvType.UNKNOWN ) {
                        throw new CodecException("itemType not valid,name="+name+",itemType="+itemType);
                    }
                    int arraycls = TlvCodec.clsToArrayType(itemConfig.cls);
                    TlvType c = new TlvType(name,arraycls,itemConfig.code,fieldInfo,itemConfig);
                    if( arraycls == TlvCodec.CLS_STRUCTARRAY) {
                        c.structNames = itemConfig.structNames;
                        c.structTypes = itemConfig.structTypes;
                        c.structLens = itemConfig.structLens;
                        c.structFieldInfos = itemConfig.structFieldInfos;
                    }

                    if( typeCodeToNameMap.get(c.code) != null ) {
                    	TlvType tlvType = typeCodeToNameMap.get(c.code);
                        if( TlvCodec.isArray(tlvType.cls) )
                            throw new CodecException("multiple array definitions for one code!!! code="+c.code+",serviceId="+serviceId);
                    }
                    typeCodeToNameMap.put(c.code,c); // overwrite itemtype's map

                    if( typeNameToCodeMap.get(c.name) != null ) {
                        throw new CodecException("name duplicated, name="+c.name+",serviceId="+serviceId);
                    }

                    typeNameToCodeMap.put(c.name,c);

                    //val classex  = codecAttributes.getOrElse("classex-"+itemType,null);
                    //if( classex != null ) {
                      //  codecAttributes.put("classex-"+name,classex);
                    //}
                    break;
                }

                case TlvCodec.CLS_STRUCT:

                {
                	ArrayList<String> structNames = new ArrayList<String>();
                	ArrayList<Integer> structTypes = new ArrayList<Integer>();
                	ArrayList<Integer> structLens = new ArrayList<Integer>();
                	ArrayList<TlvFieldInfo> structFieldInfos = new ArrayList<TlvFieldInfo>();

                	List<Element> fields = t.selectNodes("field");
            		for (Element f : fields) {

                        String fieldName = f.attributeValue("name");
                        int fieldType = TlvCodec.clsToInt( f.attributeValue("type") );

                        if( fieldType != TlvCodec.CLS_INT && fieldType != TlvCodec.CLS_STRING && fieldType != TlvCodec.CLS_SYSTEMSTRING) {
                            throw new CodecException("not supported field type,name="+name+",type="+fieldType);
                        }

                        int fieldLen = -1;
                        String lenstr = f.attributeValue("len","");
                        fieldLen =  lenstr.equals("") ? -1 : Integer.parseInt(lenstr);

                        if( fieldType == TlvCodec.CLS_INT ) fieldLen = 4;
                        if( fieldType == TlvCodec.CLS_SYSTEMSTRING ) fieldLen = 4; // special length

                        TlvFieldInfo fieldInfo2 = getTlvFieldInfo(f);

                        structNames.add(fieldName);
                        structTypes.add(fieldType);
                        structLens.add(fieldLen);
                        structFieldInfos.add(fieldInfo2);

                        //val classex = (f \ "@classex").toString;
                        //if( classex != "" ) {
                          //  codecAttributes.put("classex-"+name+"-"+fieldName,classex);
                           // codecAttributes.put("classex-"+name,"some");
                        //}
                    }

                    //val itemmetadatas = f.attributes.filter(  _.key != "name").filter( _.key != "type").filter( _.key != "len")
                    //for( m <- itemmetadatas ) {
                        //codecAttributes.put("type-"+name+"-"+fieldName+"-"+m.key,m.value.text)
                    //}

                    boolean lenOk = true;
                    for( int i = 1 ; i < structLens.size() ; ++i ) { // don't include the last field
                        if( structLens.get(i-1) == -1 ) {
                            lenOk = false;
                        }
                    }

                    if( !lenOk ) {
                        throw new CodecException("struct length not valid,name="+name+",serviceId="+serviceId);
                    }

                    TlvType c = new TlvType(name,cls,code,fieldInfo, 
                        ArrayHelper.toStringArray(structNames) ,ArrayHelper.toArray(structTypes),ArrayHelper.toArray(structLens),toTlvFieldInfoArray(structFieldInfos)
                        );

                    if( typeCodeToNameMap.get(c.code) != null ) {
                        throw new CodecException("code duplicated, code="+c.code+",serviceId="+serviceId);
                    }

                    if( typeNameToCodeMap.get(c.name) != null ) {
                        throw new CodecException("name duplicated, name="+c.name+",serviceId="+serviceId);
                    }

                    typeCodeToNameMap.put(c.code,c);
                    typeNameToCodeMap.put(c.name,c);
                    break;
                }
                
                default:
                	
                {
                	TlvType c = new TlvType(name,cls,code,fieldInfo);

                    if( typeCodeToNameMap.get(c.code) != null ) {
                        throw new CodecException("code duplicated, code="+c.code+",serviceId="+serviceId);
                    }

                    if( typeNameToCodeMap.get(c.name) != null ) {
                        throw new CodecException("name duplicated, name="+c.name+",serviceId="+serviceId);
                    }

                    typeCodeToNameMap.put(c.code,c);
                    typeNameToCodeMap.put(c.name,c);

                    //val classex  = (t \ "@classex").toString;
                    //if( classex != "" ) {
                        //codecAttributes.put("classex-"+name,classex);
                    //}
                }
            }

        }

		List<Element> messages = cfgXml.selectNodes("/service/message");
		for (Element t : messages) {

			HashMap<String,String> attributes = new HashMap<String,String>();

            int msgId = Integer.parseInt( t.attributeValue("id") );
            String msgNameOrig = t.attributeValue("name") ;
            String msgName = t.attributeValue("name").toLowerCase();

            if( msgIdToNameMap.get(msgId) != null ) {
                throw new CodecException("msgId duplicated, msgId="+msgId+",serviceId="+serviceId);
            }

            if( msgNameToIdMap.get(msgName) != null ) {
                throw new CodecException("msgName duplicated, msgName="+msgName+",serviceId="+serviceId);
            }

            msgIdToNameMap.put(msgId,msgName);
            msgNameToIdMap.put(msgName,msgId);
            msgIdToOrigNameMap.put(msgId,msgNameOrig);

            //val metadatas = t.attributes.filter(  _.key != "id").filter( _.key != "name")
            //for( m <- metadatas ) {
              //  attributes.put(m.key,m.value.text)
            //}

            // dbbroker
            //val sql = (t \ "sql" ).text.trim
            //if(sql != "")
               // attributes.put("sql",sql)

            List<Element> fieldsreq = t.selectNodes("requestParameter/field");
            
            HashMap<String,String> m1req = new HashMap<String,String>();
            HashMap<String,String> m2req = new HashMap<String,String>();
            ArrayList<String> m3req = new ArrayList<String>();

            HashMap<String,TlvFieldInfo> m4req = new HashMap<String,TlvFieldInfo>();

            for( Element f : fieldsreq ) {
                String key = f.attributeValue("name");
                String typeName = f.attributeValue("type","").toLowerCase();
                if( typeName.equals("") ) typeName = (key+"_type").toLowerCase();

                TlvType tlvType = typeNameToCodeMap.get(typeName);
                if( tlvType == null ) {
                    throw new CodecException("typeName "+typeName+" not found");
                }

                if( m1req.get(key) != null ) {
                    throw new CodecException("req key duplicated, key="+key+",serviceId="+serviceId+",msgId="+msgId);
                }
                if( m2req.get(typeName) != null ) {
                    throw new CodecException("req type duplicated, type="+typeName+",serviceId="+serviceId+",msgId="+msgId);
                }

                m1req.put(key,typeName);
                m2req.put(typeName,key);
                m3req.add(key);
                TlvFieldInfo fieldInfo = getTlvFieldInfo(f,tlvType);
                if( fieldInfo != null ) {
                    m4req.put(key,fieldInfo);
                } else if( tlvType.cls == TlvCodec.CLS_STRUCT ) {
                    if( tlvType.hasFieldInfo() ) 
                        m4req.put(key,null);
                } else if( tlvType.cls == TlvCodec.CLS_STRINGARRAY || tlvType.cls == TlvCodec.CLS_INTARRAY ) {
                    if( tlvType.itemType.fieldInfo != null ) 
                        m4req.put(key,null);
                } else if( tlvType.cls == TlvCodec.CLS_STRUCTARRAY ) {
                    if( tlvType.itemType.hasFieldInfo() ) 
                        m4req.put(key,null);
                }

                //val metadatas = f.attributes.filter(  _.key != "name").filter( _.key != "type").filter( _.key != "required")
                //for( m <- metadatas ) {
                  //  attributes.put("req-"+key+"-"+m.key,m.value.text)
                //}
            }
            msgKeyToTypeMapForReq.put(msgId,m1req);
            msgTypeToKeyMapForReq.put(msgId,m2req);
            msgKeysForReq.put(msgId,m3req);
            if( m4req.size() > 0 )
                msgKeyToFieldInfoMapForReq.put(msgId,m4req);

            List<Element> fieldsres = t.selectNodes("responseParameter/field");
            
            HashMap<String,String> m1res = new HashMap<String,String>();
            HashMap<String,String> m2res = new HashMap<String,String>();
            ArrayList<String> m3res = new ArrayList<String>();
            HashMap<String,TlvFieldInfo> m4res = new HashMap<String,TlvFieldInfo>();
            for(Element f: fieldsres ) {
                String key = f.attributeValue("name");
                String typeName = f.attributeValue("type","").toLowerCase();
                if( typeName.equals("") ) typeName = (key+"_type").toLowerCase();

                TlvType tlvType = typeNameToCodeMap.get(typeName);
                if( tlvType == null ) {
                    throw new CodecException("typeName "+typeName+" not found");
                }

                if( m1res.get(key) != null ) {
                    throw new CodecException("res key duplicated, key="+key+",serviceId="+serviceId+",msgId="+msgId);
                }
                if( m2res.get(typeName) != null ) {
                    throw new CodecException("res type duplicated, type="+typeName+",serviceId="+serviceId+",msgId="+msgId);
                }

                m1res.put(key,typeName);
                m2res.put(typeName,key);
                m3res.add(key);
                TlvFieldInfo fieldInfo = getTlvFieldInfo(f,tlvType);
                if( fieldInfo != null ) {
                    m4res.put(key,fieldInfo);
                } else if( tlvType.cls == TlvCodec.CLS_STRUCT ) {
                    if( tlvType.hasFieldInfo() ) 
                        m4res.put(key,null);
                } else if( tlvType.cls == TlvCodec.CLS_STRINGARRAY || tlvType.cls == TlvCodec.CLS_INTARRAY ) {
                    if( tlvType.itemType.fieldInfo != null ) 
                        m4res.put(key,null);
                } else if( tlvType.cls == TlvCodec.CLS_STRUCTARRAY ) {
                    if( tlvType.itemType.hasFieldInfo() ) 
                        m4res.put(key,null);
                }

                //val metadatas = f.attributes.filter(  _.key != "name").filter( _.key != "type").filter( _.key != "required")
                //for( m <- metadatas ) {
                  //  attributes.put("res-"+key+"-"+m.key,m.value.text)
                //}
            }

            msgKeyToTypeMapForRes.put(msgId,m1res);
            msgTypeToKeyMapForRes.put(msgId,m2res);
            msgKeysForRes.put(msgId,m3res);
            if( m4res.size() > 0 )
                msgKeyToFieldInfoMapForRes.put(msgId,m4res);

            msgAttributes.put(msgId,attributes);

            // if( attributes.size > 0 ) println( attributes )
        }

    }
    
    public MapWithReturnCode decodeRequest(int msgId, ByteBuffer buff,int encoding) {
        try {
        	HashMap<String,String> keyMap = msgTypeToKeyMapForReq.get(msgId);
            if( keyMap == null ) keyMap = TlvCodec.EMPTY_STRINGMAP;
            HashMap<String,Object> m = decode(keyMap,buff,encoding);
            HashMap<String,TlvFieldInfo> fieldInfoMap = msgKeyToFieldInfoMapForReq.get(msgId);
            HashMap<String,String> keyMap2 = msgKeyToTypeMapForReq.get(msgId);
            if( keyMap2 == null ) keyMap2 = TlvCodec.EMPTY_STRINGMAP;
            int ec = validateAndEncode(keyMap2,fieldInfoMap,m,true);
            return new MapWithReturnCode(m,ec);
        } catch(Exception e) {
                log.error("decode request error",e);
                return new MapWithReturnCode(new HashMap<String,Object>(),ErrorCodes.TLV_ERROR);
        }
    } 

    public MapWithReturnCode decodeResponse(int msgId, ByteBuffer buff,int encoding) {
        try {
        	HashMap<String,String> keyMap = msgTypeToKeyMapForRes.get(msgId);
        	if( keyMap == null ) keyMap = TlvCodec.EMPTY_STRINGMAP;
        	HashMap<String,Object> m = decode(keyMap,buff,encoding);
        	HashMap<String,TlvFieldInfo> fieldInfoMap = msgKeyToFieldInfoMapForRes.get(msgId);
        	HashMap<String,String> keyMap2 = msgKeyToTypeMapForRes.get(msgId);
        	if( keyMap2 == null ) keyMap2 = TlvCodec.EMPTY_STRINGMAP;
            int ec = validateAndEncode(keyMap2,fieldInfoMap,m,false);
            return new MapWithReturnCode(m,ec);
        } catch(Exception e) {
                log.error("decode response error",e);
                return new MapWithReturnCode(new HashMap<String,Object>(),ErrorCodes.TLV_ERROR);
        }
    }

    HashMap<String,Object> decode(HashMap<String,String> keyMap, ByteBuffer buff,int encoding) throws Exception {

        buff.position(0);
        int limit = buff.remaining();

        HashMap<String,Object> map = new HashMap<String,Object>();
        boolean breakFlag = false;

        while (buff.position() + 4 <= limit && !breakFlag ) {

            int code = buff.getShort();
            int len= buff.getShort() & 0xffff;
            int tlvheadlen = 4;
            if( code > 0 && len == 0 ) { len = buff.getInt(); tlvheadlen = 8; }

            if( len < tlvheadlen ) {
                if( code == 0 && len == 0 ) { // 00 03 00 04 00 00 00 00, the last 4 bytes are padding bytes by session server

                } else {
                    log.error("length_error,code="+code+",len="+len+",limit="+limit+",map="+map.toString());
                    if( log.isDebugEnabled() ) {
                        log.debug("body bytes="+toHexString(buff));
                    }
                }
                breakFlag = true;
            }
            if( buff.position() + len - tlvheadlen > limit ) {
                log.error("length_error,code="+code+",len="+len+",limit="+limit+",map="+map.toString());
                if( log.isDebugEnabled() ) {
                    log.debug("body bytes="+toHexString(buff));
                }
                breakFlag = true;
            }

            if( !breakFlag ) {

            	TlvType config = typeCodeToNameMap.get(code);
            	if( config == null ) config = TlvType.UNKNOWN;

            	String key = keyMap.get(config.name);

                // if is array, check to see if is itemType

                if( key == null && TlvCodec.isArray(config.cls) ) {
                    key = keyMap.get(config.itemType.name);
                    if( key != null ) {
                        config = config.itemType;
                    }
                }

                if( config == TlvType.UNKNOWN || key == null ) {

                    int newposition = buff.position()+aligned(len)-tlvheadlen;
                    if( newposition > buff.limit()) newposition = buff.limit();

                    buff.position(newposition);

                } else {

                    switch(config.cls) {

                        case TlvCodec.CLS_INT:
	                        {
	                            if( len != 8 ) throw new CodecException("int_length_error,len="+len);
	                            int value = buff.getInt();
	                            map.put(key,value);
	                        }
                            break;

                        case TlvCodec.CLS_STRING:
	                        {
	                            String value = new String(buff.array(),buff.position(),len-tlvheadlen,AvenueCodec.ENCODING_STRINGS[encoding]);
	
	                            map.put(key,value);
	
	                            int newposition = buff.position()+aligned(len)-tlvheadlen;
	                            if( newposition > buff.limit()) newposition = buff.limit();
	
	                            buff.position(newposition);
	                        }
                            break;

                        case TlvCodec.CLS_BYTES:
                        	{
	                            int p = buff.position();
	                            byte[] value = new byte[len-tlvheadlen];
	                            buff.get(value);
	                            map.put(key,value);
	
	                            int newposition = p+aligned(len)-tlvheadlen;
	                            if( newposition > buff.limit()) newposition = buff.limit();
	
	                            buff.position(newposition);
	                        }
                            break;

                        case TlvCodec.CLS_STRUCT:
                        	{
	                            HashMap<String,Object> value = decodeStruct(buff,len-tlvheadlen,config,encoding);
	                            map.put(key,value);
	                        }
                            break;

                        case TlvCodec.CLS_INTARRAY:
                        	{
	                            if( len != 8 ) throw new CodecException("int_length_error,len="+len);
	                            int value = buff.getInt();
	
		                        Object a = map.get(key);
		                        if( a == null ) {
		                        	ArrayList<Integer> aa = new ArrayList<Integer>();
		                            aa.add(value);
		                            map.put(key,aa);
		                        } else {
		                        	ArrayList<Integer> aa = (ArrayList<Integer>)a;
		                            aa.add(value);
		                        }
	                        }
                        	break;

                        case TlvCodec.CLS_STRINGARRAY:
                        	{
	                            String value = new String(buff.array(),buff.position(),len-tlvheadlen,AvenueCodec.ENCODING_STRINGS[encoding]);
	
	                            Object a = map.get(key);
	                            if( a == null ) {
	                            	ArrayList<String> aa = new ArrayList<String>();
	                                aa.add(value);
	                                map.put(key,aa);
	                            } else {
	                            	ArrayList<String> aa = (ArrayList<String>)a;
	                            	aa.add(value);
	                            }
	
	                            int newposition = buff.position()+aligned(len)-tlvheadlen;
	                            if( newposition > buff.limit()) newposition = buff.limit();
	
	                            buff.position(newposition);
	                        }
	                        break;

                        case TlvCodec.CLS_STRUCTARRAY:
	                        {
	                            HashMap<String,Object> value = decodeStruct(buff,len-tlvheadlen,config,encoding);
	
	                            Object a = map.get(key);
	                            if( a == null ) {
	                            	ArrayList<HashMap<String,Object>> aa = new ArrayList<HashMap<String,Object>>();
	                                aa.add(value);
	                                map.put(key,aa);
	                            } else {
	                            	ArrayList<HashMap<String,Object>> aa = (ArrayList<HashMap<String,Object>>)a;
	                            	aa.add(value);
	                            }
	                        }
	                        break;

                        default:

                           int newposition = buff.position()+aligned(len)-tlvheadlen;
                            if( newposition > buff.limit()) newposition = buff.limit();

                            buff.position(newposition);
                        }
                    }
                }
            }


        return map;
    }

    HashMap<String,Object> decodeStruct(ByteBuffer buff,int maxLen,TlvType config,int encoding) throws Exception  {

    	HashMap<String,Object> map = new HashMap<String,Object>();

        int totalLen = 0;
        for( int i= 0 ;  i <config.structNames.length ; ++i ) {

            String key = config.structNames[i];
            int t = config.structTypes[i];
            int len = config.structLens[i];

            if( len == -1 ) len = maxLen - totalLen; // last field

            totalLen += len;
            if( totalLen > maxLen ) {
                throw new CodecException("struct_data_not_valid");
            }

            switch(t) {

                case TlvCodec.CLS_INT:
	                {
	                    int value = buff.getInt();
	                    map.put(key,value);
	                }
	                break;

                case TlvCodec.CLS_STRING :
                	{
                		String value = new String(buff.array(),buff.position(),len,AvenueCodec.ENCODING_STRINGS[encoding]).trim();
	                    map.put(key,value);
	
	                    int newposition = buff.position()+aligned(len);
	                    if( newposition > buff.limit()) newposition = buff.limit();
	
	                    buff.position(newposition);
	                }
                	break;

                case TlvCodec.CLS_SYSTEMSTRING:
	                {
	                    len = buff.getInt();  // length for system string
	                    totalLen += aligned(len);
	                    if( totalLen > maxLen ) {
	                        throw new CodecException("struct_data_not_valid");
	                    }
	                    String value = new String(buff.array(),buff.position(),len,AvenueCodec.ENCODING_STRINGS[encoding]).trim();
	                    map.put(key,value);
	
	                    int newposition = buff.position()+aligned(len);
	                    if( newposition > buff.limit()) newposition = buff.limit();
	
	                    buff.position(newposition);
	                }
	                break;

                default:

                    log.error("unknown type");
            }
        }

        return map;
    }

    
    public ByteBufferWithReturnCode encodeRequest(int msgId,Map<String,Object> map,int encoding) {
        try {
        	HashMap<String,String> keyMap = msgKeyToTypeMapForReq.get(msgId);
        	if( keyMap == null ) keyMap = TlvCodec.EMPTY_STRINGMAP;
        	HashMap<String,TlvFieldInfo> fieldInfoMap = msgKeyToFieldInfoMapForReq.get(msgId);
            int ec = validateAndEncode(keyMap,fieldInfoMap,map,false);
            ByteBuffer b = encode(keyMap,map,encoding);
            return new ByteBufferWithReturnCode(b,ec);
        } catch(Exception e) {
                log.error("encode request error",e);
                return new ByteBufferWithReturnCode(EMPTY_BUFFER,ErrorCodes.TLV_ERROR);
        }
    }

    public ByteBufferWithReturnCode encodeResponse(int msgId,Map<String,Object> map,int encoding) {
        try {
        	HashMap<String,String> keyMap = msgKeyToTypeMapForRes.get(msgId);
        	if( keyMap == null ) keyMap = TlvCodec.EMPTY_STRINGMAP;
        	HashMap<String,TlvFieldInfo> fieldInfoMap = msgKeyToFieldInfoMapForRes.get(msgId);
        	int ec = validateAndEncode(keyMap,fieldInfoMap,map,true);
        	ByteBuffer b = encode(keyMap,map,encoding);
            return new ByteBufferWithReturnCode(b,ec);
        } catch(Exception e) {
                log.error("encode response error",e);
                return new ByteBufferWithReturnCode(EMPTY_BUFFER,ErrorCodes.TLV_ERROR);
        }
    }

    ByteBuffer encode(HashMap<String,String> keyMap,Map<String,Object> map,int encoding) throws Exception {

    	ArrayList<ByteBuffer> buffs = new ArrayList<ByteBuffer>(1);
        buffs.add( ByteBuffer.allocate(bufferSize) );

        
        for( Map.Entry<String,Object> entry : map.entrySet() ) {
	        	 String key = entry.getKey();
	        	 Object value = entry.getValue();
        		 
        		 if( value == null ) continue;
        		 
                 String name = keyMap.get(key);
                 if( name != null ) {

                	 TlvType config = typeNameToCodeMap.get(name);
                	 if(config == null ) config = TlvType.UNKNOWN;

                     switch(config.cls) {

                         case TlvCodec.CLS_INT: {
                             encodeInt(buffs, config.code, value);
                             break;
                         }

                         case TlvCodec.CLS_STRING: {
                             encodeString(buffs, config.code, value, encoding);
                             break;
                         }

                         case TlvCodec.CLS_BYTES: {
                             encodeBytes(buffs, config.code, value);
                             break;
                         }

                         case TlvCodec.CLS_STRUCT: {
                             encodeStruct(buffs, config, value, encoding);
                             break;
                         }

                         case TlvCodec.CLS_INTARRAY: {
                             encodeIntArray(buffs, config.code, value);
                         	 break;
                         }

                         case TlvCodec.CLS_STRINGARRAY: {
                             encodeStringArray(buffs, config.code, value, encoding);
                             break;
                         }

                         case TlvCodec.CLS_STRUCTARRAY: {
                             encodeStructArray(buffs, config, value, encoding);
                             break;
                         }

                         default:
                             ;
                         }
                     }
        }
    
    	for( ByteBuffer b: buffs ) {
    		b.flip();
    	}

        if( buffs.size() == 1 )
            return buffs.get(0);

        int total = 0;
        for( ByteBuffer b: buffs ) {
    		total += b.limit();
    	}
        
        ByteBuffer totalBuff = ByteBuffer.allocate(total);
        
        for( ByteBuffer b: buffs ) {
        	totalBuff.put(b);
    	}
        totalBuff.flip();
        buffs.clear();
        return totalBuff;
    }
    
    void encodeInt(ArrayList<ByteBuffer> buffs,int code, Object v) {
        int value = anyToInt(v);
        ByteBuffer buff = findBuff(buffs,8);
        buff.putShort((short)code);
        buff.putShort((short)8);
        buff.putInt(value);
    }

    void encodeString(ArrayList<ByteBuffer> buffs,int code, Object v, int encoding) throws Exception {
        String value = anyToString(v);
        if( value == null ) return;
        byte[] bytes = value.getBytes(AvenueCodec.ENCODING_STRINGS[encoding]);
        int tlvheadlen = 4;
        int alignedLen = aligned(bytes.length+tlvheadlen);
        if( alignedLen > 65535 && enableExtendTlv ) {
            tlvheadlen = 8;
            alignedLen = aligned(bytes.length+tlvheadlen);
        }

        ByteBuffer buff = findBuff(buffs,alignedLen);
        buff.putShort((short)code);

        if( tlvheadlen == 8 ) {
            buff.putShort((short)0);
            buff.putInt(bytes.length+tlvheadlen);
        } else {
            buff.putShort((short)(bytes.length+tlvheadlen));
        }

        buff.put(bytes);
        buff.position(buff.position() + alignedLen - bytes.length - tlvheadlen );
    }

    void encodeBytes(ArrayList<ByteBuffer> buffs,int code,Object v) {
        byte[] bytes = (byte[])v;
        int tlvheadlen = 4;
        int alignedLen = aligned(bytes.length+tlvheadlen);
        if( alignedLen > 65535 && enableExtendTlv ) {
            tlvheadlen = 8;
            alignedLen = aligned(bytes.length+tlvheadlen);
        }

        ByteBuffer buff = findBuff(buffs,alignedLen);
        buff.putShort((short)code);

        if( tlvheadlen == 8 ) {
            buff.putShort((short)0);
            buff.putInt(bytes.length+tlvheadlen);
        } else {
            buff.putShort((short)(bytes.length+tlvheadlen));
        }

        buff.put(bytes);
        buff.position(buff.position() + alignedLen - bytes.length - tlvheadlen );
    }

    void encodeStruct(ArrayList<ByteBuffer> buffs,TlvType config,Object value, int encoding) throws Exception  {

    	if( !(value instanceof Map)) { 
    		log.error("not supported type, type={}",value.getClass().getName());
	    	return;
	    }
    	
    	Map<String,Object> datamap = (Map<String,Object>)value;

		List<Object> data = new ArrayList<Object>();
        int totalLen = 0;

        for( int i = 0 ; i <config.structNames.length ; ++i ) {

            String key = config.structNames[i];
            int t = config.structTypes[i];
            int len = config.structLens[i];

            if( !datamap.containsKey(key) ) {
                String[] allKeys = config.structNames;
                String[] missedKeys = findMissedKeys(allKeys,datamap);
                throw new CodecException("struct_not_valid, struct names= "+mkString(allKeys,",")+", missed keys="+mkString(missedKeys,","));
            }

            Object v = datamap.get(key);
            if( v == null ) v = "";

            switch(t) {

                case TlvCodec.CLS_INT: {
                        totalLen += 4;
                        data.add( anyToInt(v) );
                    }
                    break;

                case TlvCodec.CLS_STRING : {

                        if(v == null) v = "";
                        byte[] s = anyToString(v).getBytes(AvenueCodec.ENCODING_STRINGS[encoding]);

                        int actuallen = s.length;
                        if( len == -1 ||  s.length == len) {
                            totalLen += s.length;
                            data.add(s);
                        } else if( s.length < len ) {
                            totalLen += len;
                            data.add(s);
                            data.add(new byte[len-s.length]); // pad zeros
                        } else {
                            throw new CodecException("string_too_long");
                        }
                        int alignedLen = aligned(len);
                        if( alignedLen != len) {
                            totalLen += (alignedLen - len) ;
                            data.add(new byte[alignedLen-len]); // pad zeros
                        }
                    }
                    break;
                
                case TlvCodec.CLS_SYSTEMSTRING: {

                        if(v == null) v = "";
                        byte[] s = anyToString(v).getBytes(AvenueCodec.ENCODING_STRINGS[encoding]);

                        int alignedLen = aligned(s.length);
                        totalLen += 4;
                        data.add(anyToInt(s.length));
                        totalLen += alignedLen;
                        data.add(s);
                        if(  s.length != alignedLen) {
                            data.add(new byte[alignedLen-s.length]); // pad zeros
                        } 
                    }
                    break;

                default:
                    log.error("unknown type");
            }

        }

        int tlvheadlen = 4;
        int alignedLen = aligned(totalLen+tlvheadlen);

        if( alignedLen > 65535 && enableExtendTlv ) {
            tlvheadlen = 8;
            alignedLen = aligned(totalLen+tlvheadlen);
        }

        ByteBuffer buff = findBuff(buffs,alignedLen);
        buff.putShort((short)config.code);

        if( tlvheadlen == 8 ) {
            buff.putShort((short)0);
            buff.putInt(totalLen+tlvheadlen);
        } else {
            buff.putShort((short)(totalLen+tlvheadlen));
        }

        for( Object v : data ) {
        	if( v instanceof byte[] ) {
        		buff.put((byte[])v);	
        	} else if( v instanceof Integer )  {
        		buff.putInt((Integer)v);	
        	}
        }

        buff.position(buff.position() + alignedLen - totalLen - tlvheadlen);
    }

    void encodeIntArray(ArrayList<ByteBuffer> buffs,int code, Object value) {

        if( value instanceof List ) {
        	List list = (List)value; 
        	for( Object o:list ) {
        		encodeInt(buffs,code,o);
        	}
        	return;
        }

        log.error("not supported type, type={}",value.getClass().getName());
    }

    void encodeStringArray(ArrayList<ByteBuffer> buffs,int code, Object value, int encoding) throws Exception {

        if( value instanceof List ) {
        	List list = (List)value; 
        	for( Object o:list ) {
        		encodeString(buffs,code, o == null ? "" : o,encoding);
        	}
        	return;
        }

        log.error("not supported type, type={}",value.getClass().getName());
    }

    void encodeStructArray(ArrayList<ByteBuffer> buffs,TlvType config,Object value, int encoding) throws Exception  {

        if( value instanceof List ) {
        	List list = (List)value; 
        	for( Object o:list ) {
        		encodeStruct(buffs,config,o,encoding);
        	}
        	return;
        }

        log.error("not supported type, type={}",value.getClass().getName());
    }

    
    int anyToInt(Object value) {
        return TypeSafe.anyToInt(value);
    }
    String anyToString(Object value) {
    	return TypeSafe.anyToString(value);
    }

    // new function
    String[] findMissedKeys(String[] allKeys,Map<String,Object> map) {
    	ArrayList<String> missedKeys = new ArrayList<String>();
    	for( String key: allKeys) {
    		if(!map.containsKey(key)) missedKeys.add(key);
    	}
    	return missedKeys.toArray(new String[0]);
    }
    
    HashMap<String,Object> anyToStruct(TlvType config,Object value) {

    	if( !(value instanceof Map) ) 
    		throw new CodecException("struct_not_valid");

    	HashMap<String,Object> retmap = new HashMap<String,Object>();
    	Map<String,Object> datamap = (Map<String,Object>)value;
        int i = 0;
        
        while( i < config.structNames.length ) {

            String key = config.structNames[i];
            int t = config.structTypes[i];
            TlvFieldInfo fieldInfo = config.structFieldInfos[i];

            if( !datamap.containsKey(key) && (fieldInfo == null || fieldInfo.defaultValue == null ) ) {
                String[] allKeys = config.structNames;
                String[] missedKeys = findMissedKeys(allKeys,datamap);
                throw new CodecException("struct_not_valid, struct names= "+mkString(allKeys,",")+", missed keys="+mkString(missedKeys,","));
            }

            Object v = datamap.get(key);

            if( v == null && fieldInfo != null && fieldInfo.defaultValue != null ) {
                v = fieldInfo.defaultValue;
            }
            if( v == null ) v = "";

            switch(t) {

                case TlvCodec.CLS_INT :
                    retmap.put(key,anyToInt(v));
                    break;
                case TlvCodec.CLS_STRING :
                    retmap.put(key,anyToString(v));
                    break;	
                case TlvCodec.CLS_SYSTEMSTRING :
                    retmap.put(key,anyToString(v));
                    break;	
            }

            i += 1;
        }
        return retmap;
          
    }

    List<Integer> anyToIntArray(Object value) {

        if( value instanceof List ) {
        	List<Integer> arr = new ArrayList<Integer>();
        	List list = (List)value;
        	for(Object o:list) {
        		arr.add( anyToInt(o));
        	}
        	return arr;
        }
        
        log.error("not supported type, type={}",value.getClass().getName());
        return null;
    }

    List<String> anyToStringArray(Object value) {

        if( value instanceof List ) {
        	List<String> arr = new ArrayList<String>();
        	List list = (List)value;
        	for(Object o:list) {
        		arr.add( anyToString(o));
        	}
        	return arr;
        }
        
        log.error("not supported type, type={}",value.getClass().getName());
        return null;
    }

    void changeTypeForMap(TlvType config, Map<String,Object> datamap) {
        int i = 0;
        while( i < config.structNames.length ) {

            String key = config.structNames[i];
            int t = config.structTypes[i];
            TlvFieldInfo fieldInfo = config.structFieldInfos[i];

            if( !datamap.containsKey(key) && (fieldInfo == null || fieldInfo.defaultValue == null ) ) {
                String[] allKeys = config.structNames;
                String[] missedKeys = findMissedKeys(allKeys,datamap);
                throw new CodecException("struct_not_valid, struct names= "+mkString(allKeys,",")+", missed keys="+mkString(missedKeys,","));
            }

            Object v = datamap.get(key);
            if( v == null && fieldInfo != null && fieldInfo.defaultValue != null ) {
                v = fieldInfo.defaultValue ;
                datamap.put(key,v);
            }
            if( v == null ) v = "";

            switch(t) {
                case TlvCodec.CLS_INT :
                    if( !(v instanceof Integer) )
                        datamap.put(key,anyToInt(v));
                case TlvCodec.CLS_STRING :
                    if( !(v instanceof String) )
                        datamap.put(key,anyToString(v));
                case TlvCodec.CLS_SYSTEMSTRING :
                    if(!(v instanceof String) )
                        datamap.put(key,anyToString(v));
            }

            i += 1;
        }
    }
    
    List<HashMap<String,Object>> anyToStructArray(TlvType config,Object value) {

        if( value instanceof List ) {
        	List<Object> list = (List<Object>)value;
        	ArrayList<HashMap<String,Object>> arr = new ArrayList<HashMap<String,Object>>();
        	for(int i=0;i<list.size();++i) {
        		arr.add( anyToStruct(config,list.get(i)) );
        	}
        	return arr;
        }
        
        log.error("not supported type, type={}",value.getClass().getName());
        return null;
    }

    ByteBuffer findBuff(ArrayList<ByteBuffer> buffs,int len) {

        // c++ requirment: array tlv must be continued, so always append to last buff 

        //val avails = buffs.filter( _.remaining >= len )
        //if( avails.size > 0 )
           // return avails(0)

    	ByteBuffer buff = buffs.get(buffs.size()-1);
        if( buff.remaining() >= len ) return buff;

        int needLen = len < bufferSize ? bufferSize : len;
        buff = ByteBuffer.allocate(needLen);
        buffs.add(buff);
        return buff;
    }

    int aligned(int len) {
        if( (len & 0x03) != 0)
            return ((len >> 2) + 1) << 2;
        else
        	return len;
    }

    public String msgIdToName(int msgId) {
        return msgIdToNameMap.get(msgId);
    }
    public int msgNameToId(String msgName) {
    	Object o = msgNameToIdMap.get(msgName);
    	if( o == null ) return 0;
    	return (Integer)o;
    }

}
