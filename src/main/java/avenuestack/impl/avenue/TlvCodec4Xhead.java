package avenuestack.impl.avenue;

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import avenuestack.impl.util.TypeSafe;
import static avenuestack.impl.avenue.TlvCodec.*;
import static avenuestack.impl.avenue.Xhead.*;

public class TlvCodec4Xhead {

	static Logger log = LoggerFactory.getLogger(TlvCodec4Xhead.class);

	public static String SPS_ID_0 = "00000000000000000000000000000000";

	public static HashMap<String, Object> decode(int serviceId, ChannelBuffer buff) {
		try {
			return decodeInternal(serviceId, buff);
		} catch (Exception e) {
			log.error("xhead decode exception, e={}", e.getMessage());
			return new HashMap<String, Object>();
		}
	}

	private static HashMap<String, Object> decodeInternal(int serviceId, ChannelBuffer buff) {

		int limit = buff.writerIndex();
		HashMap<String, Object> map = new HashMap<String, Object>();

		boolean isServiceId3 = (serviceId == 3);

		if (isServiceId3) {
			// only a "socId" field
			int len = Math.min(limit, 32);
			String value = readString(buff, len, "utf-8").trim();
			map.put(KEY_SOC_ID, value);
			return map;
		}

		boolean brk = false;

		while (buff.readerIndex() + 4 <= limit && !brk) {

			int code = (int) buff.readShort();
			int len = buff.readShort() & 0xffff;

			if (len < 4) {
				if (log.isDebugEnabled()) {
					log.debug("xhead_length_error,code=" + code + ",len=" + len + ",map=" + map + ",limit=" + limit);
					log.debug("xhead bytes=" + hexDump(buff));
				}
				brk = true;
			}
			if (buff.readerIndex() + len - 4 > limit) {
				if (log.isDebugEnabled()) {
					log.debug("xhead_length_error,code=" + code + ",len=" + len + ",map=" + map + ",limit=" + limit);
					log.debug("xhead bytes=" + hexDump(buff));
				}
				brk = true;
			}

			if (!brk) {

				XheadType tp = codeMap.get(code);
				if (tp != null) {
					if (tp.cls.equals("string"))
						decodeString(buff, len, tp.name, map);
					else if (tp.cls.equals("int"))
						decodeInt(buff, len, tp.name, map);
					else if (tp.cls.equals("addr"))
						decodeAddr(buff, len, tp.name, map);
				}
			} else {
				skipRead(buff, aligned(len) - 4);
			}
		}

		Object a = map.get("addrs");
		if (a != null) {
			ArrayList<String> aa = (ArrayList<String>) a;
			map.put(KEY_FIRST_ADDR, aa.get(0));
			map.put(KEY_LAST_ADDR, aa.get(aa.size() - 1));
		}
		return map;
	}

	private static void decodeInt(ChannelBuffer buff, int len, String key, HashMap<String, Object> map) {
		if (len != 8)
			throw new CodecException("invalid int length");

		int value = buff.readInt();
		if (!map.containsKey(key))
			map.put(key, value);
	}

	private static void decodeString(ChannelBuffer buff, int len, String key, HashMap<String, Object> map) {
		String value = readString(buff, len - 4, "utf-8");
		if (!map.containsKey(key))
			map.put(key, value);
	}

	private static void decodeAddr(ChannelBuffer buff, int len, String key, HashMap<String, Object> map) {
		if (len != 12)
			throw new CodecException("invalid addr length");

		byte[] ips = new byte[4];
		buff.readBytes(ips);
		int port = buff.readInt();

		try {
			String ipstr = InetAddress.getByAddress(ips).getHostAddress();
			String value = ipstr + ":" + port;

			Object a = map.get(key);
			if (a == null) {
				ArrayList<String> aa = new ArrayList<String>();
				aa.add(value);
				map.put(key, aa);
			} else {
				ArrayList<String> aa = (ArrayList<String>) a;
				aa.add(value);
			}
		} catch (Exception e) {
			log.error("cannot parse ip,e=" + e);
		}
	}

	public static ChannelBuffer encode(int serviceId, HashMap<String, Object> map, int version) {
		if (map == null) {
			return ChannelBuffers.buffer(0);
		}
		try {
			return encodeInternal(serviceId, map,version);
		} catch (Exception e) {
			log.error("xhead encode exception, e={}", e.getMessage());
			// throw new RuntimeException(e.getMessage);
			return ChannelBuffers.buffer(0);
		}
	}

	private static ChannelBuffer encodeInternal(int serviceId, HashMap<String, Object> map, int version) {

		Object socId = map.get(KEY_SOC_ID);

		boolean isServiceId3 = (serviceId == 3);

		if (isServiceId3) {
			if (socId != null) {
				ChannelBuffer buff = ChannelBuffers.buffer(32);
				byte[] bs = TypeSafe.anyToString(socId).getBytes();
				int len = Math.min(bs.length, 32);

				buff.writeBytes(bs, 0, len);
				for (int i = 0; i < 32 - len; ++i) {
					buff.writeByte((byte) 0);
				}
				return buff;
			} else {
				return ChannelBuffers.buffer(0);
			}
		}

		int max = maxXheadLen(version);
		ChannelBuffer buff = ChannelBuffers.dynamicBuffer(128);

		for (Map.Entry<String, Object> entry : map.entrySet()) {
			String k = entry.getKey();
			Object v = entry.getValue();
			if (v == null)
				continue;
			
			XheadType tp = nameMap.get(k);
			if (tp == null)
				continue;
			
			if (tp.cls.equals("string"))
				encodeString(buff, tp.code, v, max);
			else if (tp.cls.equals("int"))
				encodeInt(buff, tp.code, v, max);
			else if (tp.cls.equals("addr")) {
				if (v instanceof List) {
					List<String> infos = (List<String>) v;
					for (String info : infos) {
						encodeAddr(buff, tp.code, info, max);
					}
				} else {
					throw new CodecException("unknown addrs");
				}
			}
		}

		return buff;
	}

	private static void encodeAddr(ChannelBuffer buff, int code, String s, int max) {

		if (max - buff.writerIndex() < 12)
			throw new CodecException("xhead is too long");

		String[] ss = s.split(":");

		try {
			byte[] ipBytes = InetAddress.getByName(ss[0]).getAddress();

			buff.writeShort((short) code);
			buff.writeShort((short) 12);
			buff.writeBytes(ipBytes);
			buff.writeInt(Integer.parseInt(ss[1]));
		} catch (Exception e) {
			throw new CodecException("xhead InetAddress.getByName exception, e=" + e.getMessage());
		}
	}

	private static void encodeInt(ChannelBuffer buff, int code, Object v, int max) {
		if (max - buff.writerIndex() < 8)
			throw new CodecException("xhead is too long");
		int value = TypeSafe.anyToInt(v);
		buff.writeShort((short) code);
		buff.writeShort((short) 8);
		buff.writeInt(value);
	}

	private static void encodeString(ChannelBuffer buff, int code, Object v, int max) {
		String value = TypeSafe.anyToString(v);
		if (value == null)
			return;
		try {
			byte[] bytes = value.getBytes("utf-8");
			int alignedLen = aligned(bytes.length + 4);
			if (max - buff.writerIndex() < alignedLen)
				throw new CodecException("xhead is too long");
			buff.writeShort((short) code);
			buff.writeShort((short) (bytes.length + 4));
			buff.writeBytes(bytes);
			writePad(buff);
		} catch (Exception e) {
			throw new CodecException("xhead getBytes exception, e=" + e.getMessage());
		}
	}

	private static int maxXheadLen(int version) {
		if (version == 1)
			return 256 - 44;
		else
			return 1024 - 28;
	}

	public static void appendAddr(ChannelBuffer buff, String addr, boolean insertSpsId, int version) {
		int max = maxXheadLen(version);
		encodeAddr(buff, CODE_ADDRS, addr, max);
		if (insertSpsId) {
			encodeString(buff, CODE_SOC_ID, addr, max);
			encodeString(buff, CODE_SPS_ID, SPS_ID_0, max); // 预写入一个值，调用updateSpsId再更新为实际值
			String uuid = java.util.UUID.randomUUID().toString().replaceAll("-", "");
			encodeString(buff, CODE_UNIQUE_ID, uuid, max);
		}
	}

	// buff是一个完整的avenue包, 必须定位到扩展包头的开始再按code查找到CODE_SPS_ID
	public static void updateSpsId(ChannelBuffer buff, String spsId) {

		int version = (int) buff.getByte(2);
		int standardlen = version == 1 ? 44 : 28;
		int headLen0 = buff.getByte(1) & 0xff;
		int headLen = version == 1 ? headLen0 : headLen0 * 4;

		int start = standardlen;
		while (start + 4 <= headLen) {
			int code = (int) buff.getShort(start);
			int len = buff.getShort(start + 2) & 0xffff;
			if (code != CODE_SPS_ID) {
				start += aligned(len);
			} else {
				buff.setBytes(start + 4, spsId.getBytes());
				return;
			}
		}

	}
}
