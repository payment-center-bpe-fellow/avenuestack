package avenuestack;

import java.util.ArrayList;
import java.util.HashMap;

public class WrappedMap {

	private HashMap<String,Object> body;
	
	public WrappedMap(HashMap<String,Object> body) {
		this.body = body;
	}
	
	public HashMap<String,Object> get() {
		return body;
	}
	
    public String s(String name) {
    	Object v = body.get(name);
    	return anyToString(v);
    }

    public String s(String name,String defaultValue) {
    	String v = s(name);
    	if(v == null) return defaultValue;
    	return v;
    }
    
    public String ns(String name) {
    	String v = s(name);
    	if(v == null) return "";
    	return v;
    }

    public String ns(String name,String defaultValue) {
    	String v = s(name);
    	if(v == null || v.equals("")) return defaultValue;
    	return v;
    }

    public int i(String name) {
    	Object v = body.get(name);
    	return anyToInt(v);
    }

    public long l(String name) {
    	Object v = body.get(name);
    	return anyToLong(v);
    }

    public double d(String name) {
    	Object v = body.get(name);
    	return anyToDouble(v);
    }
    
    public WrappedMap m(String name) {
    	Object v = body.get(name);
    	return anyToMap(v);
    }
    
    public WrappedMap nm(String name) {
    	WrappedMap m = m(name);
    	if( m == null ) {
    		return new WrappedMap(new HashMap<String,Object>());
    	}
    	return m;
    }    

    public ArrayList<String> ls(String name) {
    	Object v = body.get(name);
    	if(v == null) return null;
    	if( v instanceof ArrayList ) {
    		ArrayList list = (ArrayList)v;
    		if( list.size() == 0 ) return list;
    		if( list.get(0) instanceof String ) return list;
    		ArrayList<String> newlist = new ArrayList<String>();
    		for( Object o : list ) {
   				newlist.add(anyToString(o));
    		}
    		return newlist;
    	}
    	return null;
    }    
    
    public ArrayList<String> nls(String name) {
    	ArrayList<String> v = ls(name);
    	if( v == null ) return new ArrayList<String>();
   		return v;
    }  
    
    public ArrayList<Integer> li(String name) {
    	Object v = body.get(name);
    	if(v == null) return null;
    	if( v instanceof ArrayList ) {
    		ArrayList list = (ArrayList)v;
    		if( list.size() == 0 ) return list;
    		if( list.get(0) instanceof Integer ) return list;
    		ArrayList<Integer> newlist = new ArrayList<Integer>();
    		for( Object o : list ) {
   				newlist.add(anyToInt(o));
    		}
    		return newlist;
    	}
    	return null;
    }    
    
    public ArrayList<Integer> nli(String name) {
    	ArrayList<Integer> v = li(name);
    	if( v == null ) return new ArrayList<Integer>();
   		return v;
    }  
    
    public ArrayList<Long> ll(String name) {
    	Object v = body.get(name);
    	if(v == null) return null;
    	if( v instanceof ArrayList ) {
    		ArrayList list = (ArrayList)v;
    		if( list.size() == 0 ) return list;
    		if( list.get(0) instanceof Long ) return list;
    		ArrayList<Long> newlist = new ArrayList<Long>();
    		for( Object o : list ) {
   				newlist.add(anyToLong(o));
    		}
    		return newlist;
    	}
    	return null;
    }    
    
    public ArrayList<Long> nll(String name) {
    	ArrayList<Long> v = ll(name);
    	if( v == null ) return new ArrayList<Long>();
   		return v;
    }  

    public ArrayList<Double> ld(String name) {
    	Object v = body.get(name);
    	if(v == null) return null;
    	if( v instanceof ArrayList ) {
    		ArrayList list = (ArrayList)v;
    		if( list.size() == 0 ) return list;
    		if( list.get(0) instanceof Double ) return list;
    		ArrayList<Double> newlist = new ArrayList<Double>();
    		for( Object o : list ) {
   				newlist.add(anyToDouble(o));
    		}
    		return newlist;
    	}
    	return null;
    }    
    
    public ArrayList<Double> nld(String name) {
    	ArrayList<Double> v = ld(name);
    	if( v == null ) return new ArrayList<Double>();
   		return v;
    }  
    
    public ArrayList<WrappedMap> lm(String name) {
    	Object v = body.get(name);
    	if(v == null) return null;
    	if( v instanceof ArrayList ) {
    		ArrayList list = (ArrayList)v;
    		if( list.size() == 0 ) return list;
    		if( list.get(0) instanceof WrappedMap ) return list;
    		if( list.get(0) instanceof HashMap ) {
        		ArrayList<WrappedMap> newlist = new ArrayList<WrappedMap>();
        		for( Object o : list ) {
       				newlist.add(anyToMap(o));
        		}
        		return newlist;
    		}
    	}
    	return null;
    }    
    
    public ArrayList<WrappedMap> nlm(String name) {
    	ArrayList<WrappedMap> v = lm(name);
    	if( v == null ) return new ArrayList<WrappedMap>();
   		return v;
    }

	public String anyToString(Object v) {
		if(v == null) return null;
		if( v instanceof String) {
			return (String)v;
		}
		return v.toString(); 	
	}

	public int anyToInt(Object v) {
		if(v == null) return 0;
		if( v instanceof Integer) {
			return (Integer)v;
		}
		if( v instanceof Number) {
			return ((Number)v).intValue();
		}    	
		try {
			return Integer.parseInt(v.toString());
		} catch(Exception e) {
			return 0;
		}    	
	}  
	public long anyToLong(Object v) {
		if(v == null) return 0;
		if( v instanceof Long) {
			return (Long)v;
		}
		if( v instanceof Number) {
			return ((Number)v).longValue();
		}    	
		try {
			return Long.parseLong(v.toString());
		} catch(Exception e) {
			return 0;
		}    	
	}  
	
	public double anyToDouble(Object v) {
		if(v == null) return 0;
		if( v instanceof Double) {
			return (Double)v;
		}
		if( v instanceof Number) {
			return ((Number)v).doubleValue();
		}    	
		try {
			return Double.parseDouble(v.toString());
		} catch(Exception e) {
			return 0;
		}    	
	}  	
	
	public WrappedMap anyToMap(Object v) {
		if(v == null) return null;
		if( v instanceof WrappedMap) {
			return (WrappedMap)v;
		}		
		if( v instanceof HashMap) {
			return new WrappedMap((HashMap<String,Object>)v);
		}
		return null; 	
	}	
}

