package avenuestack.impl.avenue;

import java.util.HashMap;

public class FieldInfo {

	static HashMap<String, FieldInfo> cache = new HashMap<String, FieldInfo>();

	static FieldInfo getTlvFieldInfo(String defaultValue, String validatorCls, String validatorParam,
			String validatorReturnCode, String encoderCls, String encoderParam) {

		if (defaultValue == null && validatorCls == null && encoderCls == null)
			return null;

		String key = "defaultValue=" + defaultValue + ",validatorCls=" + validatorCls + ",validatorParam="
				+ validatorParam + ",validatorReturnCode=" + validatorReturnCode + ",encoderCls=" + encoderCls
				+ ",encoderParam=" + encoderParam;
		FieldInfo v0 = cache.get(key);
		if (v0 != null)
			return v0;

		FieldInfo v = new FieldInfo(defaultValue, validatorCls, validatorParam, validatorReturnCode, encoderCls, encoderParam);
		cache.put(key, v);
		return v;
	}

	String defaultValue;
	String validatorCls;
	String validatorParam;
	String validatorReturnCode;
	String encoderCls;
	String encoderParam;

	Validator validator;
	Encoder encoder;

	FieldInfo(String defaultValue, String validatorCls, String validatorParam, String validatorReturnCode,
			String encoderCls, String encoderParam) {
		this.defaultValue = defaultValue;
		this.validatorCls = validatorCls;
		this.validatorParam = validatorParam;
		this.validatorReturnCode = validatorReturnCode;
		this.encoderCls = encoderCls;
		this.encoderParam = encoderParam;
		validator = Validator.getValidator(validatorCls, validatorParam, validatorReturnCode);
		encoder = Encoder.getEncoder(encoderCls, encoderParam);
	}
}
