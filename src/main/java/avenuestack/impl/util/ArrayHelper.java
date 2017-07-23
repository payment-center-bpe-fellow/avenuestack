package avenuestack.impl.util;

import java.util.ArrayList;
import java.util.HashSet;

public class ArrayHelper {
	static public String mkString(String[] ss, String interval) {
		StringBuilder b = new StringBuilder();
		int i = 0;
		for (String s : ss) {
			if (i > 0)
				b.append(interval);
			b.append(s);
			i += 1;
		}
		return b.toString();
	}

	static public String mkString(int[] ss, String interval) {
		StringBuilder b = new StringBuilder();
		int i = 0;
		for (int s : ss) {
			if (i > 0)
				b.append(interval);
			b.append(s);
			i += 1;
		}
		return b.toString();
	}

	static public String mkString(long[] ss, String interval) {
		StringBuilder b = new StringBuilder();
		int i = 0;
		for (long s : ss) {
			if (i > 0)
				b.append(interval);
			b.append(s);
			i += 1;
		}
		return b.toString();
	}

	static public String mkString(double[] ss, String interval) {
		StringBuilder b = new StringBuilder();
		int i = 0;
		for (double s : ss) {
			if (i > 0)
				b.append(interval);
			b.append(s);
			i += 1;
		}
		return b.toString();
	}

	static public String mkString(ArrayList<String> list, String interval) {
		StringBuilder b = new StringBuilder();
		int i = 0;
		for (String s : list) {
			if (i > 0)
				b.append(interval);
			b.append(s);
			i += 1;
		}
		return b.toString();
	}

	static public String mkString(HashSet<String> set, String interval) {
		StringBuilder b = new StringBuilder();
		int i = 0;
		for (String s : set) {
			if (i > 0)
				b.append(interval);
			b.append(s);
			i += 1;
		}
		return b.toString();
	}

	static public int[] toArrayInt(ArrayList<Integer> list) {
		int[] b = new int[list.size()];
		for (int i = 0; i < list.size(); ++i) {
			b[i] = list.get(i);
		}
		return b;
	}

	static public long[] toArrayLong(ArrayList<Long> list) {
		long[] b = new long[list.size()];
		for (int i = 0; i < list.size(); ++i) {
			b[i] = list.get(i);
		}
		return b;
	}

	static public double[] toArrayDouble(ArrayList<Double> list) {
		double[] b = new double[list.size()];
		for (int i = 0; i < list.size(); ++i) {
			b[i] = list.get(i);
		}
		return b;
	}

	static public String[] toStringArray(ArrayList<String> list) {
		String[] b = new String[list.size()];
		for (int i = 0; i < list.size(); ++i) {
			b[i] = list.get(i);
		}
		return b;
	}

}
