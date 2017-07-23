package avenuestack.impl.avenue;

import org.jboss.netty.buffer.ChannelBuffer;

public interface AvenueCrypt {
	ChannelBuffer encrypt(ChannelBuffer b, String key);

	ChannelBuffer decrypt(ChannelBuffer b, String key);
}
