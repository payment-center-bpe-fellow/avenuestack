package avenuestack.impl.avenue;

import java.nio.ByteBuffer;

public interface AvenueCrypt {
	ByteBuffer encrypt(ByteBuffer b,String key);
	ByteBuffer decrypt(ByteBuffer b,String key);
}

