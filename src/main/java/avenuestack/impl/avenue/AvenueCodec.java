package avenuestack.impl.avenue;

import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;

public class AvenueCodec {

	public static final int STANDARD_HEADLEN_V1 = 44;
	public static final int STANDARD_HEADLEN_V2 = 28;

	public static final int TYPE_REQUEST = 0xA1;
	public static final int TYPE_RESPONSE = 0xA2;

	public static final int FORMAT_TLV = 0;

	public static final int ENCODING_GBK = 0;
	public static final int ENCODING_UTF8 = 1;

	public static final int MUSTREACH_NO = 0;
	public static final int MUSTREACH_YES = 1;

	public static final byte ZERO = (byte) 0;
	public static final int ROUTE_FLAG = 0x0F;
	public static final int MASK = 0xff;
	public static final byte[] EMPTY_SIGNATURE = new byte[16];

	public static final int ACK_CODE = 100;

	public static AvenueCrypt avenueCrypt;

	public static int parseEncoding(String s) {
		String t = s.toLowerCase();
		if (t.equals("utf8") || t.equals("utf-8"))
			return ENCODING_UTF8;
		else
			return ENCODING_GBK;
	}

	public static String toEncoding(int enc) {
		if (enc == 0)
			return "gb18030";
		else
			return "utf-8";
	}

	public static AvenueData decode(ChannelBuffer req) {
		return decode(req, "");
	}

	public static AvenueData decode(ChannelBuffer req, String key) {

		int length = req.writerIndex();

		int flag = req.readByte() & MASK;
		int headLen0 = req.readByte() & MASK;
		int version = req.readByte() & MASK;
		req.readByte();

		if (flag != TYPE_REQUEST && flag != TYPE_RESPONSE) {
			throw new CodecException("package_type_error");
		}

		if (version != 1 && version != 2) {
			throw new CodecException("package_version_error");
		}

		int standardLen = version == 1 ? STANDARD_HEADLEN_V1 : STANDARD_HEADLEN_V2;
		int headLen = version == 2 ? headLen0 * 4 : headLen0;

		if (length < standardLen) {
			throw new CodecException("package_size_error");
		}

		if (headLen < standardLen || headLen > length) {
			throw new CodecException("package_headlen_error, headLen=" + headLen + ",length=" + length);
		}

		int packLen = req.readInt();
		if (packLen != length) {
			throw new CodecException("package_packlen_error");
		}

		int serviceId = req.readInt();
		if (serviceId < 0) {
			throw new CodecException("package_serviceid_error");
		}

		int msgId = req.readInt();
		if (msgId < 0) {
			throw new CodecException("package_msgid_error");
		}
		if (msgId == 0 && serviceId != 0) {
			throw new CodecException("package_msgid_error");
		}
		if (msgId != 0 && serviceId == 0) {
			throw new CodecException("package_msgid_error");
		}

		if (serviceId == 0 && msgId == 0) {
			if (length != standardLen) {
				throw new CodecException("package_ping_size_error");
			}
		}

		int sequence = req.readInt();

		req.readByte();

		int mustReach = req.readByte();
		int format = req.readByte();
		int encoding = req.readByte();

		if (mustReach != MUSTREACH_NO && mustReach != MUSTREACH_YES) {
			throw new CodecException("package_mustreach_error");
		}

		if (format != FORMAT_TLV) {
			throw new CodecException("package_format_error");
		}

		if (encoding != ENCODING_GBK && encoding != ENCODING_UTF8) {
			throw new CodecException("package_encoding_error");
		}

		int code = req.readInt();

		if (version == 1) {
			req.readerIndex(req.readerIndex() + 16);
		}

		int xheadlen = headLen - standardLen;
		ChannelBuffer xhead = ChannelBuffers.dynamicBuffer(xheadlen + 12);
		req.readBytes(xhead, xheadlen);
		ChannelBuffer body0 = req.readSlice(packLen - headLen);

		ChannelBuffer body = get_decrypted_body(body0, key);

		AvenueData r = new AvenueData();
		r.flag = flag;
		r.version = version;
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

	public static ChannelBuffer encode(AvenueData res) {
		return encode(res, "");
	}

	public static ChannelBuffer encode(AvenueData res, String key) {

		ChannelBuffer body = get_encrypted_body(res.body, key);

		int standardLen = res.version == 1 ? STANDARD_HEADLEN_V1 : STANDARD_HEADLEN_V2;

		int headLen0 = standardLen + res.xhead.writerIndex();
		if (res.version == 2) {
			if ((headLen0 % 4) != 0) {
				throw new CodecException("package_xhead_padding_error");
			}
		}
		int headLen = res.version == 2 ? headLen0 / 4 : headLen0;

		int packLen = standardLen + res.xhead.writerIndex() + body.writerIndex();

		ChannelBuffer b = ChannelBuffers.buffer(standardLen);

		b.writeByte(toByte(res.flag)); // type
		b.writeByte(toByte(headLen)); // headLen
		b.writeByte(toByte(res.version)); // version
		b.writeByte(toByte(ROUTE_FLAG)); // route
		b.writeInt(packLen); // packLen
		b.writeInt(res.serviceId); // serviceId
		b.writeInt(res.msgId); // msgId
		b.writeInt(res.sequence); // sequence
		b.writeByte(ZERO); // context
		b.writeByte(toByte(res.mustReach)); // mustReach
		b.writeByte(ZERO); // format
		b.writeByte(toByte(res.encoding)); // encoding

		int code = res.flag == TYPE_REQUEST ? 0 : res.code;
		b.writeInt(code);

		if (res.version == 1) {
			b.writeBytes(EMPTY_SIGNATURE); // signature
		}

		return ChannelBuffers.wrappedBuffer(b, res.xhead, body);
	}

	private static byte toByte(int i) {
		return (byte) (i & 0xff);
	}

	static ChannelBuffer get_decrypted_body(ChannelBuffer body0, String key) {
		if (key != null && !key.equals("") && avenueCrypt != null && body0 != null && body0.writerIndex() > 0) {
			return avenueCrypt.decrypt(body0, key);
		} else {
			return body0;
		}
	}

	static ChannelBuffer get_encrypted_body(ChannelBuffer body0, String key) {
		if (key != null && !key.equals("") && avenueCrypt != null && body0 != null && body0.writerIndex() > 0) {
			return avenueCrypt.encrypt(body0, key);
		} else {
			return body0;
		}
	}

}
