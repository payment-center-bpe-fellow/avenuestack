package avenuestack.impl.avenue;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;

import avenuestack.impl.util.TypeSafe;

class StructField {
	String name;
	int cls;
	int len;
	FieldInfo fieldInfo;

	StructField(String name, int cls, int len, FieldInfo fieldInfo) {
		this.name = name;
		this.cls = cls;
		this.len = len;
		this.fieldInfo = fieldInfo;
	}
}

class StructDef {
	ArrayList<StructField> fields = new ArrayList<StructField>();
	HashSet<String> keys = new HashSet<String>();
}

class ObjectDef {
	ArrayList<TlvType> fields = new ArrayList<TlvType>();
	HashMap<String, String> keyToTypeMap = new HashMap<String, String>();
	HashMap<String, String> typeToKeyMap = new HashMap<String, String>();
	HashMap<String, FieldInfo> keyToFieldMap = new HashMap<String, FieldInfo>();
}

public class TlvType {

	public static final int CLS_ARRAY = -2;
	public static final int CLS_UNKNOWN = -1;

	public static final int CLS_STRING = 1;
	public static final int CLS_INT = 2;
	public static final int CLS_LONG = 3;
	public static final int CLS_DOUBLE = 4;
	public static final int CLS_STRUCT = 5;
	public static final int CLS_OBJECT = 6;

	public static final int CLS_STRINGARRAY = 11;
	public static final int CLS_INTARRAY = 12;
	public static final int CLS_LONGARRAY = 13;
	public static final int CLS_DOUBLEARRAY = 14;
	public static final int CLS_STRUCTARRAY = 15;
	public static final int CLS_OBJECTARRAY = 16;

	public static final int CLS_BYTES = 21;
	public static final int CLS_SYSTEMSTRING = 22;
    public static final int CLS_VSTRING = 23;

	public static final TlvType UNKNOWN = new TlvType("unknown", CLS_UNKNOWN, -1, null);
	public static final StructDef EMPTY_STRUCTDEF = new StructDef();
	public static final ObjectDef EMPTY_OBJECTDEF = new ObjectDef();

	public static boolean isSimple(int cls) {
		return cls >= 1 && cls <= 4;
	}

	public static boolean isArray(int cls) {
		return cls >= 11 && cls <= 16;
	}

	public static int clsToArrayType(int cls) {
		if (cls >= 1 && cls <= 6)
			return 10 + cls;
		else
			return CLS_UNKNOWN;
	}

	public static int clsToInt(String cls) {
		return clsToInt(cls, "");
	}

	public static int clsToInt(String cls, String isbytes) {
		String lcls = cls.toLowerCase();
		if (lcls.equals("string") && TypeSafe.isTrue(isbytes))
			return CLS_BYTES;
		if (lcls.equals("string") && !TypeSafe.isTrue(isbytes))
			return CLS_STRING;
		if (lcls.equals("bytes"))
			return CLS_BYTES;
		if (lcls.equals("int"))
			return CLS_INT;
		if (lcls.equals("long"))
			return CLS_LONG;
		if (lcls.equals("double"))
			return CLS_DOUBLE;
		if (lcls.equals("struct"))
			return CLS_STRUCT;
		if (lcls.equals("object"))
			return CLS_OBJECT;
		if (lcls.equals("array"))
			return CLS_ARRAY;
		if (lcls.equals("systemstring"))
			return CLS_SYSTEMSTRING;
        if (lcls.equals("vstring"))
            return CLS_VSTRING;
		return CLS_UNKNOWN;
	}

	public static String clsToName(int cls) {
		switch (cls) {
		case CLS_STRING:
			return "string";
		case CLS_BYTES:
			return "bytes";
		case CLS_INT:
			return "int";
		case CLS_LONG:
			return "long";
		case CLS_DOUBLE:
			return "double";
		case CLS_STRUCT:
			return "struct";
		case CLS_OBJECT:
			return "object";
		case CLS_ARRAY:
			return "array";
		case CLS_SYSTEMSTRING:
			return "systemstring";
        case CLS_VSTRING:
            return "vstring";
		default:
			return "unknown cls";
		}
	}

	public static boolean checkTypeCls(int tp) {
		if ( tp == CLS_INT || tp == CLS_STRING || tp == CLS_BYTES 
		                || tp == CLS_LONG || tp == CLS_DOUBLE  )
			return true;

		return false;
	}

	public static boolean checkStructFieldCls(int fieldType) {
		if ( fieldType == CLS_INT || fieldType == CLS_STRING || fieldType == CLS_SYSTEMSTRING  || fieldType == CLS_VSTRING 
		                || fieldType == CLS_LONG  || fieldType == CLS_DOUBLE)
			return true;

		return false;
	}

	String name;
	int cls;
	int code;
	FieldInfo fieldInfo;
	TlvType itemType = UNKNOWN;
	StructDef structDef = EMPTY_STRUCTDEF;
	ObjectDef objectDef = EMPTY_OBJECTDEF;

	TlvType(String name, int cls, int code, FieldInfo fieldInfo) {
		this.name = name;
		this.cls = cls;
		this.code = code;
		this.fieldInfo = fieldInfo;
	}

	TlvType(String name, int cls, int code, FieldInfo fieldInfo, TlvType itemType, StructDef structDef,
			ObjectDef objectDef) {
		this.name = name;
		this.cls = cls;
		this.code = code;
		this.fieldInfo = fieldInfo;
		this.itemType = itemType;
		this.structDef = structDef;
		this.objectDef = objectDef;
	}

    /*
	String defaultValue() {
		if (fieldInfo == null)
			return null;
		else
			return fieldInfo.defaultValue;
	}
    */

	boolean hasSubFieldInfo() {

		switch (cls) {
		case CLS_STRINGARRAY:
			return itemType.fieldInfo != null;
		case CLS_INTARRAY:
			return itemType.fieldInfo != null;
		case CLS_LONGARRAY:
			return itemType.fieldInfo != null;
		case CLS_DOUBLEARRAY:
			return itemType.fieldInfo != null;
		case CLS_STRUCT:
			for (StructField f : structDef.fields) {
				if (f.fieldInfo != null)
					return true;
			}
			return false;
		case CLS_STRUCTARRAY:
			return itemType.fieldInfo != null || itemType.hasSubFieldInfo();
		default:
			return false;
		}
	}

}
