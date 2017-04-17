package avenuestack.impl.netty;

public interface ShakeHandsCrypt {
	String shakehands_enc_f(String pubkey,String aeskey);
	String shakehands_dec_f(String aesKey,byte[] serverKeyBytes);
}

