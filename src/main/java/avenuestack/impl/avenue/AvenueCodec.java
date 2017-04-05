package avenuestack.impl.avenue;

import java.nio.ByteBuffer;

public class AvenueCodec {

    public static final int STANDARD_HEADLEN = 44;

    public static final int TYPE_REQUEST = 0xA1;
    public static final int TYPE_RESPONSE = 0xA2;

    public static final int ROUTE_FLAG = 0x0F;

    public static final int VERSION_1 = 1;

    public static final int FORMAT_TLV = 0;
    public static final int FORMAT_JSON = 1;

    public static final int ENCODING_GBK = 0;
    public static final int ENCODING_UTF8 = 1;

    public static final String[] ENCODING_STRINGS = new String[] {"GB18030", "UTF-8" };

    public static final int MUSTREACH_NO = 0;
    public static final int MUSTREACH_YES = 1;

    public static final int ACK_CODE = 100;

    public static final int MASK = 0xff;
    public static final byte[] EMPTY_SIGNATURE = new byte[16];

    public static final String KEY_GS_INFO_FIRST = "gsInfoFirst";
    public static final String KEY_GS_INFO_LAST = "gsInfoLast";

    public static final String KEY_SOC_ID = "socId";
    public static final String KEY_GS_INFOS = "gsInfos";
    public static final String KEY_APP_ID = "appId";
    public static final String KEY_AREA_ID = "areaId";
    public static final String KEY_GROUP_ID = "groupId";
    public static final String KEY_HOST_ID = "hostId";
    public static final String KEY_SP_ID = "spId";
    public static final String KEY_ENDPOINT_ID = "endpointId"; // c++ endpointType
    public static final String KEY_UNIQUE_ID = "uniqueId"; // c++ guid
    public static final String KEY_SPS_ID = "spsId";
    public static final String KEY_HTTP_TYPE = "httpType";
    public static final String KEY_LOG_ID = "logId";

    public static final int CODE_SOC_ID = 1;
    public static final int CODE_GS_INFO = 2;
    public static final int CODE_APP_ID = 3;
    public static final int CODE_AREA_ID = 4;
    public static final int CODE_GROUP_ID = 5;
    public static final int CODE_HOST_ID = 6;
    public static final int CODE_SP_ID = 7;
    public static final int CODE_ENDPOINT_ID = 8;
    public static final int CODE_UNIQUE_ID = 9;
    public static final int CODE_SPS_ID = 11;
    public static final int CODE_HTTP_TYPE = 12;
    public static final int CODE_LOG_ID = 13;

    public static AvenueCrypt avenueCrypt; 
    
    public static int parseEncoding(String s) {
        String t = s.toLowerCase();
        if( t.equals("utf8") || t.equals("utf-8") ) return ENCODING_UTF8;
        else return ENCODING_GBK;
    }

    public static AvenueData decode(ByteBuffer req) {
    	return decode(req,"");
    }
    
    public static AvenueData decode(ByteBuffer req,String key) {

        req.position(0);

        int length = req.remaining();

        if (length < STANDARD_HEADLEN) {
            throw new CodecException("package_size_error");
        }

        int flag = req.get() & MASK;
        if (flag != TYPE_REQUEST && flag != TYPE_RESPONSE) {
            throw new CodecException("package_type_error");
        }

        int headLen = req.get() & MASK;
        if (headLen < STANDARD_HEADLEN || headLen > length) {
            throw new CodecException("package_headlen_error, headLen="+headLen+",length="+length);
        }

        int version = req.get() & MASK;
        if (version != VERSION_1) {
            throw new CodecException("package_version_error");
        }

        req.get();

        int packLen = req.getInt();
        if (packLen != length) {
            throw new CodecException("package_packlen_error");
        }

        int serviceId = req.getInt();
        if (serviceId < 0) {
            throw new CodecException("package_serviceid_error");
        }

        int msgId = req.getInt();
        if (msgId != 0) {
            if (serviceId == 0) {
                throw new CodecException("package_msgid_error");
            }
        }

        int sequence = req.getInt();

        req.get();

        int mustReach = req.get();
        int format = req.get();
        int encoding = req.get();

        if (mustReach != MUSTREACH_NO && mustReach != MUSTREACH_YES) {
            throw new CodecException("package_mustreach_error");
        }

        if (format != FORMAT_TLV && format != FORMAT_JSON) {
            throw new CodecException("package_format_error");
        }

        if (encoding != ENCODING_GBK && encoding != ENCODING_UTF8) {
            throw new CodecException("package_encoding_error");
        }

        int code = req.getInt();

        req.position(req.position() + 16);

        if (serviceId == 0 && msgId == 0) {
            if (length != STANDARD_HEADLEN) {
                throw new CodecException("package_ping_size_error");
            }
        }

        ByteBuffer xhead  = ByteBuffer.allocate(headLen - STANDARD_HEADLEN);
        req.get(xhead.array());
        xhead.position(0);

        ByteBuffer body = ByteBuffer.allocate(packLen - headLen);
        req.get(body.array());
        body.position(0);

        if( key != null && !key.equals("") && avenueCrypt != null && body != null && body.limit()>0 ) {
            body = avenueCrypt.decrypt(body,key);
        }

        AvenueData r = new AvenueData();
        r.flag = flag;
        r.serviceId = serviceId;
        r.msgId = msgId;
        r.sequence = sequence;
        r.mustReach = mustReach;
        r.encoding = encoding; 
        r.code = (flag == TYPE_REQUEST ? 0 : code);
        r.xhead = xhead;
        r.body = body;

        return r;
    }

    private static final byte ONE = (byte)1;
    private static final byte ZERO = (byte)0;

    private static byte toByte(int i) {
    	return (byte)(i&0xff);
    }

    public static ByteBuffer encode(AvenueData res) {
    	return encode(res,"");
    }
    
    public static ByteBuffer encode(AvenueData res,String key) {

    	ByteBuffer body = res.body;
    	ByteBuffer xhead = res.xhead;
    	
        if( key != null && !key.equals("") && avenueCrypt != null && body != null && body.limit()>0  ) {
            body = avenueCrypt.encrypt(body,key);
        }

    	xhead.position(0);
        body.position(0);
        int headLen = STANDARD_HEADLEN + xhead.remaining();
        int packLen = headLen+body.remaining();

        ByteBuffer b = ByteBuffer.allocate(packLen);
        b.put(toByte(res.flag)); // type
        b.put(toByte(headLen)); // headLen
        b.put(ONE); // version
        b.put(toByte(ROUTE_FLAG)); // route
        b.putInt(packLen); // packLen
        b.putInt(res.serviceId); // serviceId
        b.putInt(res.msgId); // msgId
        b.putInt(res.sequence); // sequence
        b.put(ZERO); // context
        b.put(toByte(res.mustReach)); // mustReach
        b.put(ZERO); // format
        b.put(toByte(res.encoding)); // encoding

        b.putInt( res.flag == TYPE_REQUEST ? 0 : res.code); // code
        b.put(EMPTY_SIGNATURE); // signature

        b.put(xhead);
        b.put(body);
        b.flip();

        return b;
    }

}
