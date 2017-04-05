package avenuestack.impl.avenue;

public class TlvType {

	static final TlvType UNKNOWN = new TlvType("unknown",TlvCodec.CLS_UNKNOWN,-1,null);
	static final String[] EMPTY_STRINGARRAY = new String[0];
	static final int[] EMPTY_INTARRAY = new int[0];
	static final TlvFieldInfo[] EMPTY_TLVFIELDINFOARRAY = new TlvFieldInfo[0];
	
	String name;
	int cls;
	int code;
	TlvFieldInfo fieldInfo;

    String defaultValue;

    TlvType itemType;

    String[] structNames;
    int[] structTypes;
    int[] structLens;
    TlvFieldInfo[] structFieldInfos;

    boolean hasFieldInfo() {
    	// structFieldInfos.exists( _ != null )
    	for(TlvFieldInfo i: structFieldInfos) {
    		if( i != null ) return true;
    	}
    	return false;
    }
	
	TlvType( String name, int cls, int code, TlvFieldInfo fieldInfo) {
		this.name = name;
		this.cls = cls;
		this.code = code;
		this.fieldInfo = fieldInfo;

	    defaultValue = fieldInfo == null ? null : fieldInfo.defaultValue;
	    itemType = TlvType.UNKNOWN;
	    structNames = EMPTY_STRINGARRAY;
	    structTypes = EMPTY_INTARRAY;
	    structLens = EMPTY_INTARRAY;
	    structFieldInfos = EMPTY_TLVFIELDINFOARRAY;
    }

    // constructor for struct
	TlvType( String name, int cls, int code, TlvFieldInfo fieldInfo, 
			String[] structNames,int[] structTypes,int[] structLens,TlvFieldInfo[] structFieldInfos) {
            this(name,cls,code,fieldInfo);
            this.structNames = structNames;
            this.structTypes = structTypes;
            this.structLens = structLens;
            this.structFieldInfos = structFieldInfos;
    }

    // constructor for array
    TlvType( String name, int cls, int code, TlvFieldInfo fieldInfo, TlvType itemType) {
        this(name,cls,code,fieldInfo);
        this.itemType = itemType;
    }
    /*
    override def toString() = {

        val s = new StringBuilder()

        s.append(name+","+cls+","+code)
        if( itemType != TlvType.UNKNOWN )
            s.append(","+itemType)
        else if(  structTypes != null )
            s.append(","+structNames.mkString("#")+","+structTypes.mkString("#")+","+structLens.mkString("#"))

        s.toString
    }
    */
}

