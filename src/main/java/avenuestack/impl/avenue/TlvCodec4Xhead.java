package avenuestack.impl.avenue;

import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import avenuestack.impl.util.TypeSafe;

public class TlvCodec4Xhead {
	
	static Logger log = LoggerFactory.getLogger(TlvCodec4Xhead.class);

	public static String SPS_ID_0 = "00000000000000000000000000000000";

    public static HashMap<String,Object> decode(int serviceId,ByteBuffer buff) {
        try {
        	HashMap<String,Object> m = decodeInternal(serviceId,buff);
            return m;
        } catch(Exception e){
                log.error("xhead decode exception, e={}",e.getMessage());
                return new HashMap<String,Object>();
        }
    }

    public static HashMap<String,Object> decodeInternal(int serviceId, ByteBuffer buff) {

        buff.position(0);

        int limit = buff.remaining();

        HashMap<String,Object> map = new HashMap<String,Object>();

        boolean isServiceId3 = ( serviceId == 3);

        if( isServiceId3 ) {
            // only a "socId" field
            int len = 32;
            if( limit < len ) len = limit;
            String value = new String(buff.array(),0,len).trim();
	        map.put(AvenueCodec.KEY_SOC_ID,value);
	        return map;
        }

        boolean breakFlag = false;

        while (buff.position() + 4 <= limit && !breakFlag) {

            int code = (int)buff.getShort();
            int len = buff.getShort() & 0xffff;

            if( len < 4 ) {
                if( log.isDebugEnabled() ) {
                    log.debug("xhead_length_error,code="+code+",len="+len+",map="+map+",limit="+limit);
                    log.debug("xhead bytes="+TlvCodec.toHexString(buff));
                }
                breakFlag = true;
            }
            if( buff.position() + len - 4 > limit ) {
                if( log.isDebugEnabled() ) {
                    log.debug("xhead_length_error,code="+code+",len="+len+",map="+map+",limit="+limit);
                    log.debug("xhead bytes="+TlvCodec.toHexString(buff));
                }
                breakFlag = true;
            }

            if( !breakFlag ) {
                switch(code) {

                    case AvenueCodec.CODE_GS_INFO:
                        decodeGsInfo(buff,len,AvenueCodec.KEY_GS_INFOS,map);
                        break;
                    case AvenueCodec.CODE_SOC_ID:
                        decodeString(buff,len,AvenueCodec.KEY_SOC_ID,map);
                        break;
                    case AvenueCodec.CODE_ENDPOINT_ID:
                        decodeString(buff,len,AvenueCodec.KEY_ENDPOINT_ID,map);
                        break;
                    case AvenueCodec.CODE_UNIQUE_ID:
                        decodeString(buff,len,AvenueCodec.KEY_UNIQUE_ID,map);
                        break;
                    case AvenueCodec.CODE_APP_ID:
                        decodeInt(buff,len,AvenueCodec.KEY_APP_ID,map);
                        break;
                    case AvenueCodec.CODE_AREA_ID:
                        decodeInt(buff,len,AvenueCodec.KEY_AREA_ID,map);
                        break;
                    case AvenueCodec.CODE_GROUP_ID:
                        decodeInt(buff,len,AvenueCodec.KEY_GROUP_ID,map);
                        break;
                    case AvenueCodec.CODE_HOST_ID:
                        decodeInt(buff,len,AvenueCodec.KEY_HOST_ID,map);
                        break;
                    case AvenueCodec.CODE_SP_ID:
                        decodeInt(buff,len,AvenueCodec.KEY_SP_ID,map);
                        break;
                    case AvenueCodec.CODE_SPS_ID:
                        decodeString(buff,len,AvenueCodec.KEY_SPS_ID,map);
                        break;
                    case AvenueCodec.CODE_HTTP_TYPE:
                        decodeInt(buff,len,AvenueCodec.KEY_HTTP_TYPE,map);
                        break;
                    case AvenueCodec.CODE_LOG_ID:
                        decodeString(buff,len,AvenueCodec.KEY_LOG_ID,map);
                        break;
                    default:
                        int newposition = buff.position()+aligned(len)-4;
                        if( newposition > buff.limit()) newposition = buff.limit();
                        buff.position(newposition);
                }
            }
        }

        Object a = map.get(AvenueCodec.KEY_GS_INFOS);
        if( a != null ) {
            ArrayList<String> aa = (ArrayList<String>)a;
            String firstValue = aa.get(0);
            map.put(AvenueCodec.KEY_GS_INFO_FIRST,firstValue);
            String lastValue = aa.get(aa.size()-1);
            map.put(AvenueCodec.KEY_GS_INFO_LAST,lastValue);
        }
        return map;
    }

    public static void decodeInt(ByteBuffer buff,int len,String key,HashMap<String,Object> map) {
        if( len != 8 ) return;
        int value = buff.getInt();
        if( !map.containsKey(key) ) 
            map.put(key,value);
    }
    
    public static void decodeString(ByteBuffer buff,int len,String key,HashMap<String,Object> map) {
        String value = new String(buff.array(),buff.position(),len-4);
        if( !map.containsKey(key) ) 
            map.put(key,value);
        int newposition = buff.position()+aligned(len)-4;
        if( newposition > buff.limit()) newposition = buff.limit();
        buff.position(newposition);
    }
    public static void decodeGsInfo(ByteBuffer buff,int len,String key,HashMap<String,Object> map)  {
        if( len != 12 ) return;
        byte[] ips = new byte[4];
        buff.get(ips);
        int port = buff.getInt();

        try {
	        String ipstr = InetAddress.getByAddress(ips).getHostAddress();
	        String value = ipstr + ":" + port;
	
	        Object a = map.get(key);
	        if( a == null ) {
	            ArrayList<String> aa = new ArrayList<String>();
	            aa.add(value);
	            map.put(key,aa);
	        } else {
	        	ArrayList<String> aa = (ArrayList<String>)a;
	        	aa.add(value);
	        }
        } catch(Exception e) {
        	log.error("cannot parse ip,e="+e);
        }
    }
    

    public static ByteBuffer encode(int serviceId, HashMap<String,Object> map) {
    	if( map == null ) {
    	    return ByteBuffer.allocate(0);
    	}
        try {
        	ByteBuffer buff = encodeInternal(serviceId,map);
            return buff;
        } catch(Exception e){
                log.error("xhead encode exception, e={}",e.getMessage());
                // throw new RuntimeException(e.getMessage);
                ByteBuffer buff = ByteBuffer.allocate(0);
                return buff;
        }
    }

    public static ByteBuffer encodeInternal(int serviceId,HashMap<String,Object> map)  {

        Object socId = map.get(AvenueCodec.KEY_SOC_ID);
        Object gsInfos = map.get(AvenueCodec.KEY_GS_INFOS);
        Object appId = map.get(AvenueCodec.KEY_APP_ID);
        Object areaId = map.get(AvenueCodec.KEY_AREA_ID);
        Object groupId = map.get(AvenueCodec.KEY_GROUP_ID);
        Object hostId= map.get(AvenueCodec.KEY_HOST_ID);
        Object spId = map.get(AvenueCodec.KEY_SP_ID);
        Object uniqueId = map.get(AvenueCodec.KEY_UNIQUE_ID);
        Object endpointId = map.get(AvenueCodec.KEY_ENDPOINT_ID);
        Object spsId = map.get(AvenueCodec.KEY_SPS_ID);
        Object httpType = map.get(AvenueCodec.KEY_HTTP_TYPE);
        Object logId = map.get(AvenueCodec.KEY_LOG_ID);
        boolean isServiceId3 = ( serviceId == 3);

        if( isServiceId3 ) {
            if( socId != null ) {
            	ByteBuffer buff = ByteBuffer.allocate(32);
                byte[] bs = TypeSafe.anyToString(socId).getBytes();
                int len = bs.length > 32? 32 : bs.length;

                buff.put(bs,0,len);
                for(int i = 0 ;i < 32 - len ; ++i) {
                    buff.put((byte)0);
                }
                buff.flip();
                return buff;
            } else {
            	ByteBuffer buff = ByteBuffer.allocate(0);
                return buff;
            }
        }

        ByteBuffer buff = ByteBuffer.allocate(240);

        if( gsInfos != null ) {
            
        	if( gsInfos instanceof List ) {
        		List<String> infos = (List<String>)gsInfos;
                for(String info : infos ) {
                    encodeAddr(buff,AvenueCodec.CODE_GS_INFO, info);
                }
            } else {
            	throw new CodecException("unknown gsinfos");
            }
        }
        
        if( socId != null )
            encodeString(buff,AvenueCodec.CODE_SOC_ID,socId);
        if( endpointId != null )
            encodeString(buff,AvenueCodec.CODE_ENDPOINT_ID,endpointId);
        if( uniqueId != null )
            encodeString(buff,AvenueCodec.CODE_UNIQUE_ID,uniqueId);
        if( appId != null )
            encodeInt(buff,AvenueCodec.CODE_APP_ID,appId);
        if( areaId != null )
            encodeInt(buff,AvenueCodec.CODE_AREA_ID,areaId);
        if( groupId != null )
            encodeInt(buff,AvenueCodec.CODE_GROUP_ID,groupId);
        if( hostId != null )
            encodeInt(buff,AvenueCodec.CODE_HOST_ID,hostId);
        if( spId != null )
            encodeInt(buff,AvenueCodec.CODE_SP_ID,spId);
        if( spsId != null )
            encodeString(buff,AvenueCodec.CODE_SPS_ID,spsId);
        if( httpType != null )
            encodeInt(buff,AvenueCodec.CODE_HTTP_TYPE,httpType);
        if( logId != null )
            encodeString(buff,AvenueCodec.CODE_LOG_ID,logId);

        buff.flip();
        return buff;
    }

    public static  ByteBuffer appendGsInfo(ByteBuffer buff,String gsInfo) {
    	return appendGsInfo(buff,gsInfo,false);
    }
    
    public static  ByteBuffer appendGsInfo(ByteBuffer buff,String gsInfo,boolean insertSpsId) {

        int newlen = aligned(buff.limit())+12;

        if( insertSpsId ) 
            newlen += ( aligned(gsInfo.length()) + 4 + aligned(SPS_ID_0.length())+4 );

        ByteBuffer newbuff = ByteBuffer.allocate(newlen);
        if( insertSpsId ) {
            newbuff.position(aligned(newbuff.position()));
            encodeString(newbuff,AvenueCodec.CODE_SPS_ID,SPS_ID_0);
            newbuff.position(aligned(newbuff.position()));
            encodeString(newbuff,AvenueCodec.CODE_SOC_ID,gsInfo);
        }

        newbuff.position(aligned(newbuff.position()));
        newbuff.put(buff);
        newbuff.position(aligned(newbuff.position()));
        encodeAddr(newbuff,AvenueCodec.CODE_GS_INFO,gsInfo);

        newbuff.flip();
        return newbuff;
    }


    public static  void encodeAddr(ByteBuffer buff,int code,String s) {

        if( buff.remaining() < 12 )
            throw new CodecException("xhead is too long");

        String[] ss = s.split(":");
        
        try {
	        byte[] ipBytes = InetAddress.getByName(ss[0]).getAddress();
	
	        buff.putShort((short)code);
	        buff.putShort((short)12);
	        buff.put(ipBytes);
	        buff.putInt(Integer.parseInt(ss[1]));
        } catch(Exception e) {
        	throw new CodecException("xhead InetAddress.getByName exception, e="+e.getMessage());
        }
    }

    public static void encodeInt(ByteBuffer buff,int code, Object v) {
        if( buff.remaining() < 8 )
            throw new CodecException("xhead is too long");
        int value = TypeSafe.anyToInt(v);
        buff.putShort((short)code);
        buff.putShort((short)8);
        buff.putInt(value);
    }

    public static void encodeString(ByteBuffer buff,int code,Object v)  {
        String value = TypeSafe.anyToString(v);
        if( value == null ) return;
        byte[] bytes = value.getBytes(); // don't support chinese
        int alignedLen = aligned( bytes.length+4 );
        if( buff.remaining() < alignedLen )
            throw new CodecException("xhead is too long");
        buff.putShort((short)code);
        buff.putShort((short)(bytes.length+4));
        buff.put(bytes);
        buff.position(buff.position() + alignedLen - bytes.length - 4);
    }

    public static int aligned(int len) {

        if( (len & 0x03) != 0)
            return ((len >> 2) + 1) << 2;
        else
        	return len;
    }
	
}
