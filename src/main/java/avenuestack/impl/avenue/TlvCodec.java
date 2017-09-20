package avenuestack.impl.avenue;

import java.io.FileInputStream;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.dom4j.Element;
import org.dom4j.io.SAXReader;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import avenuestack.ErrorCodes;
import avenuestack.impl.util.TypeSafe;
import static avenuestack.impl.avenue.TlvType.*;

public class TlvCodec {

	static Logger log = LoggerFactory.getLogger(TlvCodec.class);

	static String CLASSPATH_PREFIX = "classpath:";

	static HashMap<String, String> EMPTY_STRINGMAP = new HashMap<String, String>();
	static ChannelBuffer EMPTY_BUFFER = ChannelBuffers.buffer(0);
	static String CONVERTED_FLAG = "__converted__";

	static String hexDump(ChannelBuffer buff) {
		String s = ChannelBuffers.hexDump(buff, 0, buff.writerIndex());
		StringBuilder sb = new StringBuilder();
		int i = 0;
		int cnt = 0;
		sb.append("\n");
		while (i < s.length()) {
			sb.append(s.charAt(i));
			i += 1;
			cnt += 1;
			if ((cnt % 2) == 0)
				sb.append(" ");
			if ((cnt % 8) == 0)
				sb.append("  ");
			if ((cnt % 16) == 0)
				sb.append("\n");
		}
		return sb.toString();
	}

	static void writePad(ChannelBuffer buff) {
		int pad = aligned(buff.writerIndex()) - buff.writerIndex();
		switch (pad) {
		case 0:
			break;
		case 1:
			buff.writeByte(0);
			break;
		case 2:
			buff.writeByte(0);
			buff.writeByte(0);
			break;
		case 3:
			buff.writeByte(0);
			buff.writeByte(0);
			buff.writeByte(0);
			break;
		default:
			break;
		}
	}

	static void skipRead(ChannelBuffer buff, int n) {
		int newposition = Math.min(buff.readerIndex() + n, buff.writerIndex());
		buff.readerIndex(newposition);
	}

	static String readString(ChannelBuffer buff, int len, String encoding) {
		byte[] bytes = new byte[len];
		buff.getBytes(buff.readerIndex(), bytes);
		skipRead(buff, aligned(len));
		try {
			return new String(bytes, encoding);
		} catch (Exception e) {
			return "";
		}
	}

	static String getString(ChannelBuffer buff, int len, String encoding) {
		byte[] bytes = new byte[len];
		buff.getBytes(buff.readerIndex(), bytes);
		try {
			return new String(bytes, encoding);
		} catch (Exception e) {
			return "";
		}
	}

	static int aligned(int len) {
		if ((len & 0x03) != 0)
			return ((len >> 2) + 1) << 2;
		else
			return len;
	}

	String configFile;

	public int serviceId;
	public String serviceOrigName;
	public String serviceName;
	public int version = 1;

	// public ArrayList<Integer> msgIds = new ArrayList<Integer>();

	public HashMap<Integer, TlvType> typeCodeToNameMap = new HashMap<Integer, TlvType>();
	public HashMap<String, TlvType> typeNameToCodeMap = new HashMap<String, TlvType>();

	public HashMap<Integer, String> msgIdToNameMap = new HashMap<Integer, String>();
	public HashMap<String, Integer> msgNameToIdMap = new HashMap<String, Integer>();
	public HashMap<Integer, String> msgIdToOrigNameMap = new HashMap<Integer, String>();

	public HashMap<Integer, HashMap<String, String>> msgKeyToTypeMapForReq = new HashMap<Integer, HashMap<String, String>>();
	public HashMap<Integer, HashMap<String, String>> msgTypeToKeyMapForReq = new HashMap<Integer, HashMap<String, String>>();
	public HashMap<Integer, ArrayList<String>> msgKeysForReq = new HashMap<Integer, ArrayList<String>>();
	public HashMap<Integer, HashMap<String, FieldInfo>> msgKeyToFieldMapForReq = new HashMap<Integer, HashMap<String, FieldInfo>>();

	public HashMap<Integer, HashMap<String, String>> msgKeyToTypeMapForRes = new HashMap<Integer, HashMap<String, String>>();
	public HashMap<Integer, HashMap<String, String>> msgTypeToKeyMapForRes = new HashMap<Integer, HashMap<String, String>>();
	public HashMap<Integer, ArrayList<String>> msgKeysForRes = new HashMap<Integer, ArrayList<String>>();
	public HashMap<Integer, HashMap<String, FieldInfo>> msgKeyToFieldMapForRes = new HashMap<Integer, HashMap<String, FieldInfo>>();

	// public HashMap<String, String> codecAttributes = new HashMap<String,
	// String>();
	// public HashMap<Integer, HashMap<String, String>> msgAttributes = new
	// HashMap<Integer, HashMap<String, String>>();

	TlvCodec(String configFile) {
		this.configFile = configFile;
		init();
	}

	void init() {
		try {
			initInternal();
		} catch (Exception ex) {
			log.error("tlv init failed, file={}, error={}", configFile, ex.getMessage());
			throw new CodecException(ex.getMessage());
		}
	}

	String getAttribute(Element t, String field) {
		String s = t.attributeValue(field, "");
		if (s == null)
			s = "";
		return s;
	}

	String getValidatorCls(Element t) {
		String validatorCls = getAttribute(t, "validator").toLowerCase();
		if (!validatorCls.equals(""))
			return validatorCls;
		String required = getAttribute(t, "required");
		if (TypeSafe.isTrue(required))
			return "required";
		else
			return null;
	}

	FieldInfo getFieldInfo(Element t) {
		// 严格区分 null 和 空串
		String defaultValue = t.attributeValue("default");

		String validatorCls = getValidatorCls(t);
		String validatorParam = getAttribute(t, "validatorParam");
		String returnCode = getAttribute(t, "returnCode");
		String encoderCls = getAttribute(t, "encoder");
		if (TypeSafe.isEmpty(encoderCls))
			encoderCls = null;
		String encoderParam = getAttribute(t, "encoderParam");
		return FieldInfo.getTlvFieldInfo(defaultValue, validatorCls, validatorParam, returnCode, encoderCls,
				encoderParam);
	}

	FieldInfo getFieldInfo(Element t, TlvType tlvType) {

		FieldInfo fieldInfo = getFieldInfo(t);
		if (fieldInfo == null)
			return tlvType.fieldInfo;
		if (tlvType.fieldInfo == null)
			return fieldInfo;

		String defaultValue = fieldInfo.defaultValue != null ? fieldInfo.defaultValue : tlvType.fieldInfo.defaultValue;

		String validatorCls = null;
		String validatorParam = "";
		String returnCode = "";

		if (fieldInfo.validator != null) {
			validatorCls = fieldInfo.validatorCls;
			validatorParam = fieldInfo.validatorParam;
			returnCode = fieldInfo.validatorReturnCode;
		} else if (tlvType.fieldInfo.validator != null) {
			validatorCls = tlvType.fieldInfo.validatorCls;
			validatorParam = tlvType.fieldInfo.validatorParam;
			returnCode = tlvType.fieldInfo.validatorReturnCode;
		}

		String encoderCls = null;
		String encoderParam = "";

		if (fieldInfo.encoder != null) {
			encoderCls = fieldInfo.encoderCls;
			encoderParam = fieldInfo.encoderParam;
		} else if (tlvType.fieldInfo.encoder != null) {
			encoderCls = tlvType.fieldInfo.encoderCls;
			encoderParam = tlvType.fieldInfo.encoderParam;
		}

		return FieldInfo.getTlvFieldInfo(defaultValue, validatorCls, validatorParam, returnCode, encoderCls,
				encoderParam);
	}

	String getStructType(Element t) {
		String s = getAttribute(t, "type");
		if (!s.equals(""))
			return s;
		return "systemstring";
	}

	int getStructLen(Element t, int fieldType) {

		switch (fieldType) {
		case CLS_INT:
			return 4;
		case CLS_LONG:
			return 8;
		case CLS_DOUBLE:
			return 8;
		case CLS_SYSTEMSTRING:
			return 0;
        case CLS_VSTRING:
            return 0;
		case CLS_STRING:
			String s = getAttribute(t, "len");
			if (s.equals(""))
				return -1;
			else
				return Integer.parseInt(s);
		default:
			throw new CodecException("unknown type for struct, type=" + fieldType);
		}

	}

	void checkStructLen(String name, ArrayList<StructField> structFields) {
		for (int i = 0; i < structFields.size() - 1; ++i) { // don't include the
															// last field
			if (structFields.get(i).len == -1) {
				throw new CodecException("struct length not valid,name=" + name + ",serviceId=" + serviceId);
			}
		}
	}

	void initInternal() throws Exception {

		SAXReader saxReader = new SAXReader();
		saxReader.setEncoding("UTF-8");
		InputStream in;
		if (configFile.startsWith(CLASSPATH_PREFIX))
			in = TlvCodec.class.getResourceAsStream(configFile.substring(CLASSPATH_PREFIX.length()));
		else
			in = new FileInputStream(configFile);
		Element cfgXml = saxReader.read(in).getRootElement();
		in.close();

		serviceId = Integer.parseInt(cfgXml.attributeValue("id"));
		serviceOrigName = cfgXml.attributeValue("name");
		serviceName = serviceOrigName.toLowerCase();

		String versionStr = cfgXml.attributeValue("version","");
		if (!versionStr.equals(""))
			version = Integer.parseInt(versionStr);
		if (version != 1 && version != 2) {
			throw new CodecException("version not valid, serviceOrigName=" + serviceOrigName);
		}

		// ignore service metas used in scalabpe
		// ignore sql node used in scalabpe

		List<Element> types = cfgXml.selectNodes("/service/type");

		for (Element t : types) {
			String name = t.attributeValue("name").toLowerCase();
			int cls = clsToInt(t.attributeValue("class"), t.attributeValue("isbytes", ""));
			if (cls == CLS_UNKNOWN || cls == CLS_SYSTEMSTRING  || cls == CLS_VSTRING)
				throw new CodecException("class not valid, class=" + t.attributeValue("class"));
			int code = cls != CLS_ARRAY ? Integer.parseInt(t.attributeValue("code")) : 0;

			// ignore type metas used in scalabpe

			FieldInfo fieldInfo = getFieldInfo(t);

			switch (cls) {

			case CLS_ARRAY: {
				String itemTypeName = t.attributeValue("itemType", "").toLowerCase();
				TlvType itemTlvType = typeNameToCodeMap.get(itemTypeName);
				if (itemTlvType == null)
					itemTlvType = UNKNOWN;
				if (itemTlvType == UNKNOWN) {
					throw new CodecException("itemType not valid,name=" + name + ",itemType=" + itemTypeName);
				}
				int arraycls = clsToArrayType(itemTlvType.cls);
				if (arraycls == CLS_UNKNOWN) {
					throw new CodecException("itemType not valid,name=" + name + ",itemType=" + itemTypeName);
				}
				TlvType tlvType = new TlvType(name, arraycls, itemTlvType.code, fieldInfo, itemTlvType, itemTlvType.structDef,
						itemTlvType.objectDef);

				if (typeCodeToNameMap.get(tlvType.code) != null) {
					TlvType foundTlvType = typeCodeToNameMap.get(tlvType.code);
					if (isArray(foundTlvType.cls))
						throw new CodecException("multiple array definitions for one code!!! code=" + tlvType.code
								+ ",serviceId=" + serviceId);
				}
				typeCodeToNameMap.put(tlvType.code, tlvType); // overwrite itemtype's map

				if (typeNameToCodeMap.get(tlvType.name) != null) {
					throw new CodecException("name duplicated, name=" + tlvType.name + ",serviceId=" + serviceId);
				}

				typeNameToCodeMap.put(tlvType.name, tlvType);

				// ignore classex used in scalabpe
				break;
			}

			case CLS_STRUCT: {
				ArrayList<StructField> structFields = new ArrayList<StructField>();

				List<Element> fields = t.selectNodes("field");
				for (Element f : fields) {

					String fieldName = f.attributeValue("name");
					String fieldTypeName = getStructType(f);
					int fieldType = clsToInt(fieldTypeName);

					if (!checkStructFieldCls(fieldType)) {
						throw new CodecException("not supported field type,name=" + name + ",type=" + fieldType);
					}

					int fieldLen = getStructLen(f, fieldType);
					FieldInfo fieldInfo2 = getFieldInfo(f);

					StructField sf = new StructField(fieldName, fieldType, fieldLen, fieldInfo2);
					structFields.add(sf);

					// ignore classex used in scalabpe
					// ignore item metas used in scalabpe
				}

				checkStructLen(name, structFields);

				StructDef structDef = new StructDef();
				structDef.fields.addAll(structFields);
				for (StructField f : structFields)
					structDef.keys.add(f.name);
				TlvType tlvType = new TlvType(name, cls, code, fieldInfo, UNKNOWN, structDef, EMPTY_OBJECTDEF);

				if (typeCodeToNameMap.get(tlvType.code) != null) {
					throw new CodecException("code duplicated, code=" + tlvType.code + ",serviceId=" + serviceId);
				}

				if (typeNameToCodeMap.get(tlvType.name) != null) {
					throw new CodecException("name duplicated, name=" + tlvType.name + ",serviceId=" + serviceId);
				}

				typeCodeToNameMap.put(tlvType.code, tlvType);
				typeNameToCodeMap.put(tlvType.name, tlvType);
				break;
			}

			case CLS_OBJECT: {

				HashMap<String, String> t_keyToTypeMap = new HashMap<String, String>();
				HashMap<String, String> t_typeToKeyMap = new HashMap<String, String>();
				ArrayList<TlvType> t_tlvTypes = new ArrayList<TlvType>();
				HashMap<String, FieldInfo> t_keyToFieldMap = new HashMap<String, FieldInfo>();

				List<Element> fields = t.selectNodes("field");
				for (Element f : fields) {

					String key = f.attributeValue("name");
					String typeName0 = f.attributeValue("type","").toLowerCase();
					String typeName = typeName0.equals("") ? (key + "_type").toLowerCase() : typeName0;

					TlvType tlvType = typeNameToCodeMap.get(typeName);
					if (tlvType == null) {
						throw new CodecException("typeName "+typeName+" not found");
					}

					if (t_keyToTypeMap.get(key) != null) {
						throw new CodecException("key duplicated, key=" + key + ",serviceId=" + serviceId);
					}
					if (t_typeToKeyMap.get(typeName) != null) {
						throw new CodecException("type duplicated, type=" + typeName + ",serviceId=" + serviceId);
					}

					t_keyToTypeMap.put(key, typeName);
					t_typeToKeyMap.put(typeName, key);
					t_tlvTypes.add(tlvType);
					FieldInfo fieldInfo2 = getFieldInfo(f, tlvType);
					if (fieldInfo2 != null) {
						t_keyToFieldMap.put(key, fieldInfo2);
					}

					// ignore classex used in scalabpe
					// ignore item metas used in scalabpe
				}

				ObjectDef objectDef = new ObjectDef();
				objectDef.fields.addAll(t_tlvTypes);
				objectDef.keyToTypeMap.putAll(t_keyToTypeMap);
				objectDef.typeToKeyMap.putAll(t_typeToKeyMap);
				objectDef.keyToFieldMap.putAll(t_keyToFieldMap);

				TlvType tlvType = new TlvType(name, cls, code, fieldInfo, UNKNOWN, EMPTY_STRUCTDEF, objectDef);

				if (typeCodeToNameMap.get(tlvType.code) != null) {
					throw new CodecException("code duplicated, code=" + tlvType.code + ",serviceId=" + serviceId);
				}

				if (typeNameToCodeMap.get(tlvType.name) != null) {
					throw new CodecException("name duplicated, name=" + tlvType.name + ",serviceId=" + serviceId);
				}

				typeCodeToNameMap.put(tlvType.code, tlvType);
				typeNameToCodeMap.put(tlvType.name, tlvType);
				break;
			}
			default: {

				if (!checkTypeCls( cls)) {
					throw new CodecException("not supported type,name=" + name + ",type=" + clsToName(cls));
				}

				TlvType c = new TlvType(name, cls, code, fieldInfo);

				if (typeCodeToNameMap.get(c.code) != null) {
					throw new CodecException("code duplicated, code=" + c.code + ",serviceId=" + serviceId);
				}

				if (typeNameToCodeMap.get(c.name) != null) {
					throw new CodecException("name duplicated, name=" + c.name + ",serviceId=" + serviceId);
				}

				typeCodeToNameMap.put(c.code, c);
				typeNameToCodeMap.put(c.name, c);

				// ignore classex used in scalabpe
				break;
			}
			}
			String arrayTypeName = t.attributeValue("array","").toLowerCase();
			if (!arrayTypeName.equals("") && cls != CLS_ARRAY) {

				TlvType itemTlvType = typeNameToCodeMap.get(name);
				if (itemTlvType == null)
					itemTlvType = UNKNOWN;
				int arraycls = clsToArrayType(itemTlvType.cls);
				if (arraycls == CLS_UNKNOWN) {
                    throw new CodecException("not allowed class for array");
				}
				TlvType tlvType = new TlvType(arrayTypeName, arraycls, itemTlvType.code, null, itemTlvType,
						itemTlvType.structDef, itemTlvType.objectDef);

				if (typeCodeToNameMap.get(tlvType.code) != null) {
					TlvType foundTlvType = typeCodeToNameMap.get(tlvType.code);
					if (isArray(foundTlvType.cls))
						throw new CodecException("multiple array definitions for one code!!! code=" + tlvType.code
								+ ",serviceId=" + serviceId);
				}
				typeCodeToNameMap.put(tlvType.code, tlvType); // overwrite
																// itemtype's
																// map

				if (typeNameToCodeMap.get(tlvType.name) != null) {
					throw new CodecException("name duplicated, name=" + tlvType.name + ",serviceId=" + serviceId);
				}
				typeNameToCodeMap.put(tlvType.name, tlvType);
			}
		}

		List<Element> messages = cfgXml.selectNodes("/service/message");
		for (Element t : messages) {

			// HashMap<String, String> attributes = new HashMap<String,
			// String>();

			int msgId = Integer.parseInt(t.attributeValue("id"));
			String msgNameOrig = t.attributeValue("name");
			String msgName = msgNameOrig.toLowerCase();

			// ignore msgIds used in scalabpe

			if (msgIdToNameMap.get(msgId) != null) {
				throw new CodecException("msgId duplicated, msgId=" + msgId + ",serviceId=" + serviceId);
			}

			if (msgNameToIdMap.get(msgName) != null) {
				throw new CodecException("msgName duplicated, msgName=" + msgName + ",serviceId=" + serviceId);
			}

			msgIdToNameMap.put(msgId, msgName);
			msgNameToIdMap.put(msgName, msgId);
			msgIdToOrigNameMap.put(msgId, msgNameOrig);

			// ignore message metas used in scalabpe
			// ignore sql node used in scalabpe

			List<Element> fieldsreq = t.selectNodes("requestParameter/field");

			HashMap<String, String> t_keyToTypeMapForReq = new HashMap<String, String>();
			HashMap<String, String> t_typeToKeyMapForReq = new HashMap<String, String>();
			ArrayList<String> t_keysForReq = new ArrayList<String>();
			HashMap<String, FieldInfo> t_keyToFieldMapForReq = new HashMap<String, FieldInfo>();

			for (Element f : fieldsreq) {
				String key = f.attributeValue("name");
				String typeName = f.attributeValue("type", "").toLowerCase();
				if (typeName.equals(""))
					typeName = (key + "_type").toLowerCase();

				TlvType tlvType = typeNameToCodeMap.get(typeName);
				if (tlvType == null) {
					throw new CodecException("typeName " + typeName + " not found");
				}

				if (t_keyToTypeMapForReq.get(key) != null) {
					throw new CodecException(
							"req key duplicated, key=" + key + ",serviceId=" + serviceId + ",msgId=" + msgId);
				}
				if (t_typeToKeyMapForReq.get(typeName) != null) {
					throw new CodecException(
							"req type duplicated, type=" + typeName + ",serviceId=" + serviceId + ",msgId=" + msgId);
				}

				t_keyToTypeMapForReq.put(key, typeName);
				t_typeToKeyMapForReq.put(typeName, key);
				t_keysForReq.add(key);
				FieldInfo fieldInfo = getFieldInfo(f, tlvType);
				if (fieldInfo != null) {
					t_keyToFieldMapForReq.put(key, fieldInfo);
				} else if (tlvType.hasSubFieldInfo()) {
					t_keyToFieldMapForReq.put(key, null);
				}

				// ignore field metas used in scalabpe
			}
			msgKeyToTypeMapForReq.put(msgId, t_keyToTypeMapForReq);
			msgTypeToKeyMapForReq.put(msgId, t_typeToKeyMapForReq);
			msgKeysForReq.put(msgId, t_keysForReq);
			if (t_keyToFieldMapForReq.size() > 0)
				msgKeyToFieldMapForReq.put(msgId, t_keyToFieldMapForReq);

			List<Element> fieldsres = t.selectNodes("responseParameter/field");

			HashMap<String, String> t_keyToTypeMapForRes = new HashMap<String, String>();
			HashMap<String, String> t_typeToKeyMapForRes = new HashMap<String, String>();
			ArrayList<String> t_keysForRes = new ArrayList<String>();
			HashMap<String, FieldInfo> t_keyToFieldMapForRes = new HashMap<String, FieldInfo>();

			for (Element f : fieldsres) {
				String key = f.attributeValue("name");
				String typeName = f.attributeValue("type", "").toLowerCase();
				if (typeName.equals(""))
					typeName = (key + "_type").toLowerCase();

				TlvType tlvType = typeNameToCodeMap.get(typeName);
				if (tlvType == null) {
					throw new CodecException("typeName " + typeName + " not found");
				}

				if (t_keyToTypeMapForRes.get(key) != null) {
					throw new CodecException(
							"res key duplicated, key=" + key + ",serviceId=" + serviceId + ",msgId=" + msgId);
				}
				if (t_typeToKeyMapForRes.get(typeName) != null) {
					throw new CodecException(
							"res type duplicated, type=" + typeName + ",serviceId=" + serviceId + ",msgId=" + msgId);
				}

				t_keyToTypeMapForRes.put(key, typeName);
				t_typeToKeyMapForRes.put(typeName, key);
				t_keysForRes.add(key);
				FieldInfo fieldInfo = getFieldInfo(f, tlvType);
				if (fieldInfo != null) {
					t_keyToFieldMapForRes.put(key, fieldInfo);
				} else if (tlvType.hasSubFieldInfo()) {
					t_keyToFieldMapForRes.put(key, null);
				}

				// ignore field metas used in scalabpe
			}

			msgKeyToTypeMapForRes.put(msgId, t_keyToTypeMapForRes);
			msgTypeToKeyMapForRes.put(msgId, t_typeToKeyMapForRes);
			msgKeysForRes.put(msgId, t_keysForRes);
			if (t_keyToFieldMapForRes.size() > 0)
				msgKeyToFieldMapForRes.put(msgId, t_keyToFieldMapForRes);
		}

	}

	public ArrayList<TlvType> allTlvTypes() {
		ArrayList<TlvType> list = new ArrayList<TlvType>();
		for (TlvType i : typeNameToCodeMap.values())
			list.add(i);
		return list;
	}

	public ArrayList<Integer> allMsgIds() {
		ArrayList<Integer> list = new ArrayList<Integer>();
		for (Integer i : msgKeyToTypeMapForReq.keySet())
			list.add(i);
		return list;
	}

	public TlvType findTlvType(int code) {
		return typeCodeToNameMap.get(code);
	}

	public String msgIdToName(int msgId) {
		return msgIdToNameMap.get(msgId);
	}

	public int msgNameToId(String msgName) {
		Object o = msgNameToIdMap.get(msgName);
		if (o == null)
			return 0;
		return (Integer) o;
	}

	public MapWithReturnCode decodeRequest(int msgId, ChannelBuffer buff, int encoding) {
		try {
			HashMap<String, String> typeMap = msgTypeToKeyMapForReq.get(msgId);
			if (typeMap == null)
				typeMap = EMPTY_STRINGMAP;
			HashMap<String, String> keyMap = msgKeyToTypeMapForReq.get(msgId);
			if (keyMap == null)
				keyMap = EMPTY_STRINGMAP;
			HashMap<String, FieldInfo> fieldMap = msgKeyToFieldMapForReq.get(msgId);
			return decode(typeMap, keyMap, fieldMap, 
                    buff, 0, buff.writerIndex(), encoding, true);
		} catch (Exception e) {
			log.error("decode request error", e);
			return new MapWithReturnCode(new HashMap<String, Object>(), ErrorCodes.TLV_ERROR);
		}
	}

	public MapWithReturnCode decodeResponse(int msgId, ChannelBuffer buff, int encoding) {
		try {
			HashMap<String, String> typeMap = msgTypeToKeyMapForRes.get(msgId);
			if (typeMap == null)
				typeMap = EMPTY_STRINGMAP;
			HashMap<String, String> keyMap = msgKeyToTypeMapForRes.get(msgId);
			if (keyMap == null)
				keyMap = EMPTY_STRINGMAP;
			HashMap<String, FieldInfo> fieldMap = msgKeyToFieldMapForRes.get(msgId);
			return decode(typeMap, keyMap, fieldMap, 
                    buff, 0, buff.writerIndex(), encoding, false);
		} catch (Exception e) {
			log.error("decode request error", e);
			return new MapWithReturnCode(new HashMap<String, Object>(), ErrorCodes.TLV_ERROR);
		}
	}

	MapWithReturnCode decode(HashMap<String, String> typeMap, HashMap<String, String> keyMap,
			HashMap<String, FieldInfo> fieldMap, ChannelBuffer buff, int start, int limit, int encoding,
			boolean needEncode) throws Exception {

		buff.readerIndex(start);

		HashMap<String, Object> map = new HashMap<String, Object>();
		boolean brk = false;
		int errorCode = 0;

		while (buff.readerIndex() + 4 <= limit && !brk) {

			int code = buff.readShort();
			int len = buff.readShort() & 0xffff;
			int tlvheadlen = 4;
			if (code > 0 && len == 0) {
				len = buff.readInt();
				tlvheadlen = 8;
			}

			if (len < tlvheadlen) {
				if (code == 0 && len == 0) { // 00 03 00 04 00 00 00 00, the
												// last 4 bytes are padding
												// bytes by session server

				} else {
					log.error(
							"length_error,code=" + code + ",len=" + len + ",limit=" + limit + ",map=" + map.toString());
					if (log.isDebugEnabled()) {
						log.debug("body bytes=" + hexDump(buff));
					}
				}
				brk = true;
			}
			if (buff.readerIndex() + len - tlvheadlen > limit) {
				log.error("length_error,code=" + code + ",len=" + len + ",limit=" + limit + ",map=" + map.toString());
				if (log.isDebugEnabled()) {
					log.debug("body bytes=" + hexDump(buff));
				}
				brk = true;
			}

			if (!brk) {

				TlvType tlvType = typeCodeToNameMap.get(code);
				if (tlvType == null)
					tlvType = UNKNOWN;

				String key = typeMap.get(tlvType.name);

				// if is array, check to see if is itemType

				if (key == null && isArray(tlvType.cls)) {
					key = typeMap.get(tlvType.itemType.name);
					if (key != null) {
						tlvType = tlvType.itemType;
					}
				}

				if (tlvType == UNKNOWN || key == null) {

					skipRead(buff, aligned(len) - tlvheadlen);

				} else {

					switch (tlvType.cls) {

					case CLS_INT: {
						if (len != 8)
							throw new CodecException("int_length_error,len=" + len);
						int value = buff.readInt();
						map.put(key, value);
						break;
					}

					case CLS_LONG: {
						if (len != 12)
							throw new CodecException("long_length_error,len=" + len);
						long value = buff.readLong();
						map.put(key, value);
						break;
					}

					case CLS_DOUBLE: {
						if (len != 12)
							throw new CodecException("double_length_error,len=" + len);
						double value = buff.readDouble();
						map.put(key, value);
						break;
					}

					case CLS_STRING: {
						String value = readString(buff, len - tlvheadlen, AvenueCodec.toEncoding(encoding));
						map.put(key, value);
						break;
					}

					case CLS_BYTES: {
						int p = buff.readerIndex();
						byte[] value = new byte[len - tlvheadlen];
						buff.readBytes(value);
						map.put(key, value);

						int newposition = Math.min(p + aligned(len) - tlvheadlen, buff.writerIndex());
						buff.readerIndex(newposition);
						break;
					}

					case CLS_STRUCT: {
						HashMap<String, Object> value = decodeStruct(buff, len - tlvheadlen, tlvType, encoding);
						map.put(key, value);
						break;
					}

					case CLS_OBJECT: {
						int p = buff.readerIndex();
						MapWithReturnCode mrc = decode(tlvType.objectDef.typeToKeyMap, tlvType.objectDef.keyToTypeMap,
								tlvType.objectDef.keyToFieldMap, buff, p, p + len - tlvheadlen, encoding, needEncode);
						map.put(key, mrc.body);
						if (mrc.ec != 0 && errorCode == 0)
							errorCode = mrc.ec;

						int newposition = Math.min(p + aligned(len) - tlvheadlen, buff.writerIndex());
						buff.readerIndex(newposition);
						break;
					}

					case CLS_INTARRAY: {
						if (len != 8)
							throw new CodecException("int_length_error,len=" + len);
						int value = buff.readInt();

						Object a = map.get(key);
						if (a == null) {
							ArrayList<Integer> aa = new ArrayList<Integer>();
							aa.add(value);
							map.put(key, aa);
						} else {
							ArrayList<Integer> aa = (ArrayList<Integer>) a;
							aa.add(value);
						}
						break;
					}
					case CLS_LONGARRAY: {
						if (len != 12)
							throw new CodecException("long_length_error,len=" + len);
						long value = buff.readLong();

						Object a = map.get(key);
						if (a == null) {
							ArrayList<Long> aa = new ArrayList<Long>();
							aa.add(value);
							map.put(key, aa);
						} else {
							ArrayList<Long> aa = (ArrayList<Long>) a;
							aa.add(value);
						}
						break;
					}
					case CLS_DOUBLEARRAY: {
						if (len != 12)
							throw new CodecException("double_length_error,len=" + len);
						double value = buff.readDouble();

						Object a = map.get(key);
						if (a == null) {
							ArrayList<Double> aa = new ArrayList<Double>();
							aa.add(value);
							map.put(key, aa);
						} else {
							ArrayList<Double> aa = (ArrayList<Double>) a;
							aa.add(value);
						}
						break;
					}
					case CLS_STRINGARRAY: {
						String value = readString(buff, len - tlvheadlen, AvenueCodec.toEncoding(encoding));

						Object a = map.get(key);
						if (a == null) {
							ArrayList<String> aa = new ArrayList<String>();
							aa.add(value);
							map.put(key, aa);
						} else {
							ArrayList<String> aa = (ArrayList<String>) a;
							aa.add(value);
						}

						break;
					}

					case CLS_STRUCTARRAY: {
						HashMap<String, Object> value = decodeStruct(buff, len - tlvheadlen, tlvType, encoding);

						Object a = map.get(key);
						if (a == null) {
							ArrayList<HashMap<String, Object>> aa = new ArrayList<HashMap<String, Object>>();
							aa.add(value);
							map.put(key, aa);
						} else {
							ArrayList<HashMap<String, Object>> aa = (ArrayList<HashMap<String, Object>>) a;
							aa.add(value);
						}
						break;
					}
					case CLS_OBJECTARRAY: {

						int p = buff.readerIndex();

						MapWithReturnCode mrc = decode(tlvType.objectDef.typeToKeyMap, tlvType.objectDef.keyToTypeMap,
								tlvType.objectDef.keyToFieldMap, buff, p, len - tlvheadlen, encoding, needEncode);

						Object a = map.get(key);
						if (a == null) {
							ArrayList<HashMap<String, Object>> aa = new ArrayList<HashMap<String, Object>>();
							aa.add(mrc.body);
							map.put(key, aa);
						} else {
							ArrayList<HashMap<String, Object>> aa = (ArrayList<HashMap<String, Object>>) a;
							aa.add(mrc.body);
						}

						if (mrc.ec != 0 && errorCode == 0)
							errorCode = mrc.ec;

						int newposition = Math.min(p + aligned(len) - tlvheadlen, buff.writerIndex());
						buff.readerIndex(newposition);
						break;
					}

					default: {
						skipRead(buff, aligned(len) - tlvheadlen);
						break;
					}
					}
				}
			}
		}

		int ec = validate(keyMap, fieldMap, map, needEncode, false);
		if (ec != 0 && errorCode == 0)
			errorCode = ec;
		return new MapWithReturnCode(map, errorCode);
	}

	HashMap<String, Object> decodeStruct(ChannelBuffer buff, int maxLen, TlvType tlvType, int encoding)
			throws Exception {

		HashMap<String, Object> map = new HashMap<String, Object>();

		int totalLen = 0;
		for (int i = 0; i < tlvType.structDef.fields.size(); ++i) {
			StructField f = tlvType.structDef.fields.get(i);
			String key = f.name;
			int t = f.cls;
			int len = f.len;

			if (len == -1)
				len = maxLen - totalLen; // last field

			totalLen += len;
			if (totalLen > maxLen) {
				throw new CodecException("struct_data_not_valid");
			}

			switch (t) {

			case CLS_INT: {
				int value = buff.readInt();
				map.put(key, value);
				break;
			}
			case CLS_LONG: {
				long value = buff.readLong();
				map.put(key, value);
				break;
			}
			case CLS_DOUBLE: {
				double value = buff.readDouble();
				map.put(key, value);
				break;
			}
			case CLS_STRING: {
				String value = readString(buff, len, AvenueCodec.toEncoding(encoding)).trim();
				map.put(key, value);
                break;
			}
            case CLS_SYSTEMSTRING: {
                len = buff.readInt(); // length for system string
                totalLen += 4;
                totalLen += aligned(len);
                if (totalLen > maxLen) {
                    throw new CodecException("struct_data_not_valid");
                }
                String value = readString(buff, len, AvenueCodec.toEncoding(encoding)).trim();
                map.put(key, value);
                break;
            }			
            case CLS_VSTRING: {
				int t2 = buff.readShort() & 0xffff;
				if (t2 == 0xffff) {
					len = buff.readInt();
					totalLen += aligned(6 + len);
					if (totalLen > maxLen) {
						throw new CodecException("struct_data_not_valid");
					}
					String value = getString(buff, len, AvenueCodec.toEncoding(encoding)).trim();
					map.put(key, value);

					skipRead(buff, aligned(6 + len) - 6);
				} else {
					len = t2;
					totalLen += aligned(2 + len);
					if (totalLen > maxLen) {
						throw new CodecException("struct_data_not_valid");
					}
					String value = getString(buff, len, AvenueCodec.toEncoding(encoding)).trim();
					map.put(key, value);

					skipRead(buff, aligned(2 + len) - 2);
				}
				break;
			}
			default: {
				log.error("unknown type");
				break;
			}
			}
		}

		return map;
	}

	public BufferWithReturnCode encodeRequest(int msgId, Map<String, Object> map, int encoding) {
		try {
			HashMap<String, String> keyMap = msgKeyToTypeMapForReq.get(msgId);
			if (keyMap == null)
				keyMap = TlvCodec.EMPTY_STRINGMAP;
			HashMap<String, FieldInfo> fieldMap = msgKeyToFieldMapForReq.get(msgId);
			ChannelBuffer buff = ChannelBuffers.dynamicBuffer(256);
			int ec = encode(buff, keyMap, fieldMap, map, encoding, false);
			return new BufferWithReturnCode(buff, ec);
		} catch (Exception e) {
			log.error("encode request error", e);
			return new BufferWithReturnCode(EMPTY_BUFFER, ErrorCodes.TLV_ERROR);
		}
	}

	public BufferWithReturnCode encodeResponse(int msgId, Map<String, Object> map, int encoding) {
		try {
			HashMap<String, String> keyMap = msgKeyToTypeMapForRes.get(msgId);
			if (keyMap == null)
				keyMap = TlvCodec.EMPTY_STRINGMAP;
			HashMap<String, FieldInfo> fieldMap = msgKeyToFieldMapForRes.get(msgId);
			ChannelBuffer buff = ChannelBuffers.dynamicBuffer(256);
			int ec = encode(buff, keyMap, fieldMap, map, encoding, true);
			return new BufferWithReturnCode(buff, ec);
		} catch (Exception e) {
			log.error("encode response error", e);
			return new BufferWithReturnCode(EMPTY_BUFFER, ErrorCodes.TLV_ERROR);
		}
	}

	int encode(ChannelBuffer buff, HashMap<String, String> keyMap, HashMap<String, FieldInfo> fieldMap,
			Map<String, Object> dataMap, int encoding, boolean needEncode) throws Exception {

		int errorCode = 0;
		int ec = validate(keyMap, fieldMap, dataMap, needEncode, false);
		if (ec != 0 && errorCode == 0)
			errorCode = ec;

		for (Map.Entry<String, Object> entry : dataMap.entrySet()) {
			String key = entry.getKey();
			Object value = entry.getValue();

			if (value == null)
				continue;

			String typeName = keyMap.get(key);
			if (typeName != null) {

				TlvType tlvType = typeNameToCodeMap.get(typeName);
				if (tlvType == null)
					tlvType = UNKNOWN;

				switch (tlvType.cls) {

				case CLS_INT: {
					encodeInt(buff, tlvType.code, value);
					break;
				}
				case CLS_LONG: {
					encodeLong(buff, tlvType.code, value);
					break;
				}
				case CLS_DOUBLE: {
					encodeDouble(buff, tlvType.code, value);
					break;
				}

				case CLS_STRING: {
					encodeString(buff, tlvType.code, value, encoding);
					break;
				}

				case CLS_STRUCT: {
					encodeStruct(buff, tlvType, value, encoding);
					break;
				}

				case CLS_OBJECT: {
					ec = encodeObject(buff, tlvType, value, encoding, needEncode);
					if (ec != 0 && errorCode == 0)
						errorCode = ec;
					break;
				}

				case CLS_INTARRAY: {
					ec = encodeArray(buff, tlvType, value, encoding, needEncode);
					if (ec != 0 && errorCode == 0)
						errorCode = ec;
					break;
				}
				case CLS_LONGARRAY: {
					ec = encodeArray(buff, tlvType, value, encoding, needEncode);
					if (ec != 0 && errorCode == 0)
						errorCode = ec;
					break;
				}
				case CLS_DOUBLEARRAY: {
					ec = encodeArray(buff, tlvType, value, encoding, needEncode);
					if (ec != 0 && errorCode == 0)
						errorCode = ec;
					break;
				}
				case CLS_STRINGARRAY: {
					ec = encodeArray(buff, tlvType, value, encoding, needEncode);
					if (ec != 0 && errorCode == 0)
						errorCode = ec;
					break;
				}
				case CLS_STRUCTARRAY: {
					ec = encodeArray(buff, tlvType, value, encoding, needEncode);
					if (ec != 0 && errorCode == 0)
						errorCode = ec;
					break;
				}
				case CLS_OBJECTARRAY: {
					ec = encodeArray(buff, tlvType, value, encoding, needEncode);
					if (ec != 0 && errorCode == 0)
						errorCode = ec;
					break;
				}
				case CLS_BYTES: {
                    if (!(value instanceof byte[] ))
                        throw new CodecException("not supported type for bytes");

					encodeBytes(buff, tlvType.code, value);
					break;
				}

				default:
					break;
				}
			}
		}

		return errorCode;
	}

	void encodeInt(ChannelBuffer buff, int code, Object v) {
		int value = TypeSafe.anyToInt(v);
		buff.writeShort((short) code);
		buff.writeShort((short) 8);
		buff.writeInt(value);
	}

	void encodeLong(ChannelBuffer buff, int code, Object v) {
		long value = TypeSafe.anyToLong(v);
		buff.writeShort((short) code);
		buff.writeShort((short) 12);
		buff.writeLong(value);
	}

	void encodeDouble(ChannelBuffer buff, int code, Object v) {
		double value = TypeSafe.anyToDouble(v);
		buff.writeShort((short) code);
		buff.writeShort((short) 12);
		buff.writeDouble(value);
	}

	void encodeString(ChannelBuffer buff, int code, Object v, int encoding) throws Exception {
		String value = TypeSafe.anyToString(v);
		if (value == null)
			return;
		byte[] bytes = value.getBytes(AvenueCodec.toEncoding(encoding));
		int tlvheadlen = 4;
		int alignedLen = aligned(bytes.length + tlvheadlen);
		if (alignedLen > 65535) {
			tlvheadlen = 8;
			alignedLen = aligned(bytes.length + tlvheadlen);
		}

		buff.writeShort((short) code);

		if (tlvheadlen == 8) {
			buff.writeShort((short) 0);
			buff.writeInt(bytes.length + tlvheadlen);
		} else {
			buff.writeShort((short) (bytes.length + tlvheadlen));
		}

		buff.writeBytes(bytes);
		writePad(buff);
	}

	void encodeBytes(ChannelBuffer buff, int code, Object v) {
		byte[] bytes = (byte[]) v;
		int tlvheadlen = 4;
		int alignedLen = aligned(bytes.length + tlvheadlen);
		if (alignedLen > 65535) {
			tlvheadlen = 8;
			alignedLen = aligned(bytes.length + tlvheadlen);
		}

		buff.writeShort((short) code);

		if (tlvheadlen == 8) {
			buff.writeShort((short) 0);
			buff.writeInt(bytes.length + tlvheadlen);
		} else {
			buff.writeShort((short) (bytes.length + tlvheadlen));
		}

		buff.writeBytes(bytes);
		writePad(buff);
	}

	void encodeStruct(ChannelBuffer buff, TlvType tlvType, Object o, int encoding) throws Exception {

		if (!(o instanceof Map)) {
			throw new CodecException("not supported type in encodeObject, type=" + o.getClass().getName());

		}

		Map<String, Object> datamap = (Map<String, Object>) o;

		buff.writeShort((short) tlvType.code);
		buff.writeShort(0);
		int ps = buff.writerIndex();

		for (int i = 0; i < tlvType.structDef.fields.size(); ++i) {
			StructField f = tlvType.structDef.fields.get(i);

			String key = f.name;
			int t = f.cls;
			int len = f.len;

			if (!datamap.containsKey(key)) {
				throw new CodecException("struct_not_valid, key not found, key=" + key);
			}

			Object v = datamap.get(key);
			if (v == null)
				v = "";

			switch (t) {

			case CLS_INT: {
				buff.writeInt(TypeSafe.anyToInt(v));
				break;
			}
			case CLS_LONG: {
				buff.writeLong(TypeSafe.anyToLong(v));
				break;
			}
			case CLS_DOUBLE: {
				buff.writeDouble(TypeSafe.anyToDouble(v));
				break;
			}

			case CLS_STRING: {
 
				//if (v == null)
				//	v = "";
				byte[] s = TypeSafe.anyToString(v).getBytes(AvenueCodec.toEncoding(encoding));

				//int actuallen = s.length;
				if (len == -1 || s.length == len) {
					buff.writeBytes(s);
				} else if (s.length < len) {
					buff.writeBytes(s);
					buff.writeBytes(new byte[len - s.length]);
				} else {
					throw new CodecException("string_too_long");
				}
				int alignedLen = aligned(len);
				if (alignedLen != len) {
					buff.writeBytes(new byte[alignedLen - len]); // pad zeros
				}
				break;
			}
            case CLS_SYSTEMSTRING: {

                //if (v == null)
                  //  v = "";
                byte[] s = TypeSafe.anyToString(v).getBytes(AvenueCodec.toEncoding(encoding));

                buff.writeInt(TypeSafe.anyToInt(s.length));
                buff.writeBytes(s);

                int alignedLen = aligned(s.length);
                if (s.length != alignedLen) {
                    buff.writeBytes(new byte[alignedLen - s.length]);
                }
                break;
            }			
			case CLS_VSTRING: {

				//if (v == null)
				//	v = "";
				byte[] s = TypeSafe.anyToString(v).getBytes(AvenueCodec.toEncoding(encoding));

				if (s.length >= 65535) { // 0xffff
					int alignedLen = aligned(6 + s.length);
					buff.writeShort((short) 65535);
					buff.writeInt(TypeSafe.anyToInt(s.length));
					buff.writeBytes(s);
					if (alignedLen - s.length - 6 > 0) {
						buff.writeBytes(new byte[alignedLen - s.length - 6]);
					}
				} else {
					int alignedLen = aligned(2 + s.length);
					buff.writeShort((short) TypeSafe.anyToInt(s.length));
					buff.writeBytes(s);
					if (alignedLen - s.length - 2 > 0) {
						buff.writeBytes(new byte[alignedLen - s.length - 2]);
					}
				}
 
				break;
			}
			default: {
				log.error("unknown type");
				break;
			}
			}

		}

		int totalLen = buff.writerIndex() - ps;
		int tlvheadlen = 4;
		int alignedLen = aligned(totalLen + tlvheadlen);

		if (alignedLen > 65535) {
			tlvheadlen = 8;
			alignedLen = aligned(totalLen + tlvheadlen);
		}

		if (tlvheadlen == 4) {
			buff.setShort(ps - 2, (short) (totalLen + tlvheadlen));
		} else {
			buff.writeInt(0);
			int pe = buff.writerIndex();
			int i = 0;
			while (i < totalLen) {
				buff.setByte(pe - 1 - i, buff.getByte(pe - 1 - i - 4));
				i += 1;
			}

			buff.setShort(ps - 2, 0);
			buff.setInt(ps, totalLen + tlvheadlen);
		}

		writePad(buff);
	}

	int encodeObject(ChannelBuffer buff, TlvType tlvType, Object v, int encoding, boolean needEncode) throws Exception {

		if (!(v instanceof Map)) {
			throw new CodecException("not supported type in encodeObject, type=" + v.getClass().getName());
		}

		Map<String, Object> datamap = (Map<String, Object>) v;

		buff.writeShort((short) tlvType.code);
		buff.writeShort(0);
		int ps = buff.writerIndex();

		int ec = encode(buff, tlvType.objectDef.keyToTypeMap, tlvType.objectDef.keyToFieldMap, datamap, encoding,
				needEncode);

		int totalLen = buff.writerIndex() - ps;

		int tlvheadlen = 4;
		int alignedLen = aligned(totalLen + tlvheadlen);

		if (alignedLen > 65535) {
			tlvheadlen = 8;
			alignedLen = aligned(totalLen + tlvheadlen);
		}

		if (tlvheadlen == 4) {
			buff.setShort(ps - 2, (short) (totalLen + tlvheadlen));
		} else {
			buff.writeInt(0);
			int pe = buff.writerIndex();
			int i = 0;
			while (i < totalLen) {
				buff.setByte(pe - 1 - i, buff.getByte(pe - 1 - i - 4));
				i += 1;
			}

			buff.setShort(ps - 2, 0);
			buff.setInt(ps, totalLen + tlvheadlen);
		}

		writePad(buff);

		return ec;
	}

	int encodeArray(ChannelBuffer buff, TlvType tlvType, Object o, int encoding, boolean needEncode) throws Exception {

		if (o instanceof List) {
			List list = (List) o;

			int errorCode = 0;
			for (Object v : list) {
				switch (tlvType.cls) {
				case CLS_INTARRAY:
					encodeInt(buff, tlvType.code, v);
					break;
				case CLS_LONGARRAY:
					encodeLong(buff, tlvType.code, v);
					break;
				case CLS_DOUBLEARRAY:
					encodeDouble(buff, tlvType.code, v);
					break;
				case CLS_STRINGARRAY:
					encodeString(buff, tlvType.code, v, encoding);
					break;
				case CLS_STRUCTARRAY:
					encodeStruct(buff, tlvType, v, encoding);
					break;
				case CLS_OBJECTARRAY:
					int ec = encodeObject(buff, tlvType, v, encoding, needEncode);
					if (ec != 0 && errorCode == 0)
						errorCode = ec;
					break;
				}
			}
			return errorCode;
		}

		throw new CodecException("not supported type in encodeArray, type=" + o.getClass().getName());

	}

	// ignore filter functions used in scalabpe

	HashMap<String, Object> anyToStruct(TlvType tlvType, Object value, boolean addConvertFlag) {

		if (!(value instanceof Map))
			throw new CodecException("struct_not_valid");

		HashMap<String, Object> datamap = (HashMap<String, Object>) value;
		fastConvertStruct(tlvType, datamap, addConvertFlag);
		return datamap;
	}

	// ignore anyToObject used in scalabpe

	ArrayList<Integer> anyToIntArray(Object value) {

		if (value instanceof ArrayList) {
			ArrayList list = (ArrayList) value;
			if (list.size() > 0 && list.get(0) instanceof Integer) {
				return list;
			}
		}

		if (value instanceof List) {
			ArrayList<Integer> arr = new ArrayList<Integer>();
			List list = (List) value;
			for (Object o : list) {
				arr.add(TypeSafe.anyToInt(o));
			}
			return arr;
		}

		throw new CodecException("not supported type, type=" + value.getClass().getName());
	}

	ArrayList<Long> anyToLongArray(Object value) {

		if (value instanceof ArrayList) {
			ArrayList list = (ArrayList) value;
			if (list.size() > 0 && list.get(0) instanceof Long) {
				return list;
			}
		}

		if (value instanceof List) {
			ArrayList<Long> arr = new ArrayList<Long>();
			List list = (List) value;
			for (Object o : list) {
				arr.add(TypeSafe.anyToLong(o));
			}
			return arr;
		}

		throw new CodecException("not supported type, type=" + value.getClass().getName());
	}

	ArrayList<Double> anyToDoubleArray(Object value) {

		if (value instanceof ArrayList) {
			ArrayList list = (ArrayList) value;
			if (list.size() > 0 && list.get(0) instanceof Double) {
				return list;
			}
		}

		if (value instanceof List) {
			ArrayList<Double> arr = new ArrayList<Double>();
			List list = (List) value;
			for (Object o : list) {
				arr.add(TypeSafe.anyToDouble(o));
			}
			return arr;
		}

		throw new CodecException("not supported type, type=" + value.getClass().getName());
	}

	ArrayList<String> anyToStringArray(Object value) {

		if (value instanceof ArrayList) {
			ArrayList list = (ArrayList) value;
			if (list.size() > 0 && list.get(0) instanceof String) {
				return list;
			}
		}

		if (value instanceof List) {
			ArrayList<String> arr = new ArrayList<String>();
			List list = (List) value;
			for (Object o : list) {
				arr.add(TypeSafe.anyToString(o));
			}
			return arr;
		}

		throw new CodecException("not supported type, type=" + value.getClass().getName());
	}

	ArrayList<HashMap<String, Object>> anyToStructArray(TlvType tlvType, Object value, boolean addConvertFlag) {

		if (value instanceof ArrayList) {
			ArrayList list = (ArrayList) value;
			if (list.size() > 0 && list.get(0) instanceof HashMap) {
				for (Object o : list) {
					HashMap<String, Object> m = (HashMap<String, Object>) o;
					fastConvertStruct(tlvType, m, addConvertFlag);
				}
				return list;
			}
		}

		if (value instanceof List) {
			ArrayList<HashMap<String, Object>> arr = new ArrayList<HashMap<String, Object>>();
			List list = (List) value;
			for (Object o : list) {
				arr.add(anyToStruct(tlvType, o, addConvertFlag));
			}
			return arr;
		}

		throw new CodecException("not supported type, type=" + value.getClass().getName());
	}

	void fastConvertStruct(TlvType tlvType, HashMap<String, Object> datamap, boolean addConvertFlag) {

		//if (datamap.containsKey(CONVERTED_FLAG)) {
		//	datamap.remove(CONVERTED_FLAG);
		//	return;
		//}

		ArrayList<String> to_be_removed = null;
		for (String k : datamap.keySet()) {
			if (!tlvType.structDef.keys.contains(k)) {
				if (to_be_removed == null)
					to_be_removed = new ArrayList<String>();
				to_be_removed.add(k);
			}
		}
		if (to_be_removed != null) {
			for (String k : to_be_removed)
				datamap.remove(k);
		}

		for (StructField f : tlvType.structDef.fields) {

			String key = f.name;

			if (!datamap.containsKey(key)) {
				throw new CodecException("struct_not_valid, key not found, key=" + key);
			}

			Object v = datamap.get(key);
			if (v == null) {
				switch (f.cls) {
				case CLS_STRING:
					datamap.put(key, "");
					break;
				case CLS_SYSTEMSTRING:
					datamap.put(key, "");
					break;
                case CLS_VSTRING:
                    datamap.put(key, "");
                    break;
				case CLS_INT:
					datamap.put(key, 0);
					break;
				case CLS_LONG:
					datamap.put(key, 0L);
					break;
				case CLS_DOUBLE:
					datamap.put(key, 0.0);
					break;
				}
			} else {
				switch (f.cls) {
				case CLS_STRING:
					if (!(v instanceof String))
						datamap.put(key, TypeSafe.anyToString(v));
					break;
				case CLS_SYSTEMSTRING:
					if (!(v instanceof String))
						datamap.put(key, TypeSafe.anyToString(v));
					break;
                case CLS_VSTRING:
                    if (!(v instanceof String))
                        datamap.put(key, TypeSafe.anyToString(v));
                    break;
				case CLS_INT:
					if (!(v instanceof Integer))
						datamap.put(key, TypeSafe.anyToInt(v));
					break;
				case CLS_LONG:
					if (!(v instanceof Long))
						datamap.put(key, TypeSafe.anyToLong(v));
					break;
				case CLS_DOUBLE:
					if (!(v instanceof Double))
						datamap.put(key, TypeSafe.anyToDouble(v));
					break;
				}
			}

		}

		//if (addConvertFlag) {
		//	datamap.put(CONVERTED_FLAG, "1");
		//}

	}

	// ignore anyToObjectArray used in scalabpe

	int validate(HashMap<String, String> keyMap, HashMap<String, FieldInfo> fieldMap, Map<String, Object> dataMap,
			boolean needEncode, boolean addConvertFlag) {

		if (fieldMap == null || fieldMap.size() == 0)
			return 0;

		int errorCode = 0;

		for (Map.Entry<String, FieldInfo> entry : fieldMap.entrySet()) {
			String key = entry.getKey();
			FieldInfo fieldInfo = entry.getValue();

			String typeName = keyMap.get(key);
			TlvType tlvType = typeNameToCodeMap.get(typeName);

			Object value = dataMap.get(key);
			if (value == null) {
				if (fieldInfo != null && fieldInfo.defaultValue != null) {
					dataMap.put(key, safeConvertValue(fieldInfo.defaultValue, tlvType.cls));
				}
			} else {
				switch (tlvType.cls) {
				case CLS_STRUCT: {
                    if( value instanceof HashMap) {
                        HashMap<String, Object> m = (HashMap<String, Object>) value;
                        setStructDefault(m, tlvType);
                    }
					break;
				}
				case CLS_STRUCTARRAY: {
                    if( value instanceof ArrayList) {
                        ArrayList<HashMap<String, Object>> lm = (ArrayList<HashMap<String, Object>>) value;
                        for (HashMap<String, Object> m2 : lm)
                            setStructDefault(m2, tlvType);
                    }
                    break;
				}
				}
			}

			value = dataMap.get(key);
			ObjectWithReturnCode owr0 = validateValue(value, fieldInfo, needEncode);
			if (owr0.ec != 0 && errorCode == 0)
				errorCode = owr0.ec;

			if (owr0.v != null) {

				if (owr0.v != value) {
					value = safeConvertValue(owr0.v, tlvType.cls);
					dataMap.put(key, value);
				}

				switch (tlvType.cls) {
				case CLS_STRINGARRAY: {
					ArrayList<String> l = anyToStringArray(value);
					for (int i = 0; i < l.size(); ++i) {
						ObjectWithReturnCode owr = validateValue(l.get(i), tlvType.itemType.fieldInfo, needEncode);
						if (owr.ec != 0 && errorCode == 0)
							errorCode = owr.ec;
						l.set(i, TypeSafe.anyToString(owr.v));
					}
					dataMap.put(key, l);
					break;
				}
				case CLS_INTARRAY: {
					ArrayList<Integer> l = anyToIntArray(value);
					for (int i = 0; i < l.size(); ++i) {
						ObjectWithReturnCode owr = validateValue(l.get(i), tlvType.itemType.fieldInfo, needEncode);
						if (owr.ec != 0 && errorCode == 0)
							errorCode = owr.ec;
						l.set(i, TypeSafe.anyToInt(owr.v));
					}
					dataMap.put(key, l);
					break;
				}
				case CLS_LONGARRAY: {
					ArrayList<Long> l = anyToLongArray(value);
					for (int i = 0; i < l.size(); ++i) {
						ObjectWithReturnCode owr = validateValue(l.get(i), tlvType.itemType.fieldInfo, needEncode);
						if (owr.ec != 0 && errorCode == 0)
							errorCode = owr.ec;
						l.set(i, TypeSafe.anyToLong(owr.v));
					}
					dataMap.put(key, l);
					break;
				}
				case CLS_DOUBLEARRAY: {
					ArrayList<Double> l = anyToDoubleArray(value);
					for (int i = 0; i < l.size(); ++i) {
						ObjectWithReturnCode owr = validateValue(l.get(i), tlvType.itemType.fieldInfo, needEncode);
						if (owr.ec != 0 && errorCode == 0)
							errorCode = owr.ec;
						l.set(i, TypeSafe.anyToDouble(owr.v));
					}
					dataMap.put(key, l);
					break;
				}
				case CLS_STRUCT: {
					HashMap<String, Object> m = anyToStruct(tlvType, value, addConvertFlag);
					int ec = validateStruct(m, tlvType, needEncode);
					if (ec != 0 && errorCode == 0)
						errorCode = ec;
					dataMap.put(key, m);
					break;
				}
				case CLS_STRUCTARRAY: {
					ArrayList<HashMap<String, Object>> l = anyToStructArray(tlvType, value, addConvertFlag);
					for (int i = 0; i < l.size(); ++i) {
						int ec = validateStruct(l.get(i), tlvType.itemType, needEncode);
						if (ec != 0 && errorCode == 0)
							errorCode = ec;
					}
					dataMap.put(key, l);
					break;
				}
				default: {
					break;
				}
				}
			}

		}

		return errorCode;
	}

	ObjectWithReturnCode validateValue(Object v, FieldInfo fieldInfo, boolean needEncode) {
		if (fieldInfo == null)
			return new ObjectWithReturnCode(v, 0);
		int errorCode = 0;
		Object b = v;
		if (fieldInfo.validator != null) {
			int ret = fieldInfo.validator.validate(v);
			if (ret != 0) {
				log.error("validate_value_error, value=" + v + ", ret=" + ret);
				if (errorCode == 0)
					errorCode = ret;
			}
		}
		if (needEncode && fieldInfo.encoder != null) {
			b = fieldInfo.encoder.encode(v);
		}
		return new ObjectWithReturnCode(b, errorCode);
	}

	int validateStruct(Map<String, Object> map, TlvType tlvType, boolean needEncode) {

		int errorCode = 0;
		for (StructField f : tlvType.structDef.fields) {
			FieldInfo fieldInfo = f.fieldInfo;
			if (fieldInfo != null) {
				String key = f.name;
				Object value = map.get(key);
				if (fieldInfo.validator != null) {
					int ret = fieldInfo.validator.validate(value);
					if (ret != 0) {
						log.error("validate_map_error, key=" + key + ", value=" + value + ", ret=" + ret);
						if (errorCode == 0)
							errorCode = ret;
					}
				}
				if (needEncode && fieldInfo.encoder != null) {
					Object v = fieldInfo.encoder.encode(value);
					map.put(key, v);
				}
			}
		}
		return errorCode;
	}

	void setStructDefault(Map<String, Object> map, TlvType tlvType) {
		for (StructField f : tlvType.structDef.fields) {
			FieldInfo fieldInfo = f.fieldInfo;
			if (fieldInfo != null && fieldInfo.defaultValue != null) {
				String key = f.name;
				Object value = map.get(key);
				if (value == null) {
					map.put(key, safeConvertValue(fieldInfo.defaultValue, f.cls));
				}
			}
		}
	}

	Object safeConvertValue(Object v, int cls) {
		switch (cls) {
		case CLS_STRING:
			return TypeSafe.anyToString(v);
		case CLS_SYSTEMSTRING:
			return TypeSafe.anyToString(v);
        case CLS_VSTRING:
            return TypeSafe.anyToString(v);
		case CLS_INT:
			return TypeSafe.anyToInt(v);
		case CLS_LONG:
			return TypeSafe.anyToLong(v);
		case CLS_DOUBLE:
			return TypeSafe.anyToDouble(v);
		default:
			return v;
		}
	}

}
