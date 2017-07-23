package avenuestack.impl.avenue;

import java.util.HashMap;

public class MapWithReturnCode {
	public HashMap<String, Object> body;
	public int ec;

	public MapWithReturnCode(HashMap<String, Object> body, int ec) {
		this.body = body;
		this.ec = ec;
	}
}
