package avenuestack.impl.avenue;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.regex.Pattern;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import avenuestack.ErrorCodes;

/*

validator定义为：

1、	validator系列标签可以出现在<type>内 或者请求/响应中的<field>内 或 struct里的 <field>内, 请求响应中的配置优先于type上的配置
2、	validator系列标签有3个：validator 、validatorParam、returnCode分别表示验证名、验证参数、验证失败后的返回码
3、	若请求字段校验失败，直接返回错误码。若响应字段校验失败，包体不变，包头code为修改为错误码。原响应包code!=0时不触发校验。

大类	validator	validatorParam	    参数说明	    returnCode	    实现说明

必填	Required    不需要	            不需要	        默认-10242400	用于判断必填，其中空字符串算做有值
正则类	Regex	    配置为正则表达式	是否符合正则	默认-10242400	最基础的validator
        Email	    不需要	            不需要	        默认-10242400	通过正则内置实现，等价于正则参数：([0-9A-Za-z\\-_\\.]+)@([a-zA-Z0-9_-])+(.[a-zA-Z0-9_-])+
        Url	        不需要	            不需要	        默认-10242400	通过正则内置实现
范围类 	NumberRange	数字1,数字2	        左闭由闭区间	默认-10242400	用于判断整数范围
        LengthRange	数字1,数字2	        左闭由闭区间	默认-10242400	用于判断字符串长度范围
        TimeRange	字符串1,字符串2	    左闭由闭区间	默认-10242400	用于判断时间范围 示例：2011-1-2 13:00:05.231,2018-2-3 15:00:00.345
集合类 	NumberSet	A|b|C|d|e|…	                     默认-10242400	 用于整数枚举类型，示例 0|1
        Regex	    A|b|C|d|e|…		                 默认-10242400 	完全同正则validator，普通正则实现 
 
*/
public abstract class Validator {

	static Logger log = LoggerFactory.getLogger(Validator.class);
	
	static HashMap<String,Validator> cache = new HashMap<String,Validator>();
	static String emailRegex = "^[A-Za-z0-9_.-]+@[a-zA-Z0-9_-]+[.][a-zA-Z0-9_-]+$";
	static String urlRegex = "^https?://[^/]+/.*$";

	static public Validator getValidator(String cls,String param,String returnCode) {
        if( cls == null || cls.equals("") ) return null;
        int rc =  ( returnCode == null || returnCode.equals("") ) ? ErrorCodes.TLV_ERROR : Integer.parseInt(returnCode);

        String key = cls.toLowerCase() + ":param=" + param + ":rc=" + rc;
        Validator v = cache.get(key);
	    if( v != null ) return v;
	
	    cls = cls.toLowerCase();
	    if( cls.equals("required"))
	            v = new RequireValidator(cls.toLowerCase(),param,rc);
	    else if( cls.equals("regex"))
	            v = new RegexValidator(cls.toLowerCase(),param,rc);
	    else if( cls.equals("email"))
   	    		v = new RegexValidator(cls.toLowerCase(),emailRegex,rc);
	    else if( cls.equals("url"))
	    	    v = new RegexValidator(cls.toLowerCase(),urlRegex,rc);
	    else if( cls.equals("numberrange"))
	            v = new NumberRangeValidator(cls.toLowerCase(),param,rc);
	    else if( cls.equals("lengthrange"))
	            v = new LengthRangeValidator(cls.toLowerCase(),param,rc);
	    else if( cls.equals("timerange"))
	            v = new TimeRangeValidator(cls.toLowerCase(),param,rc);
	    else if( cls.equals("numberset"))
	            v = new ValueSetValidator(cls.toLowerCase(),param,rc);
	    else if( cls.equals("valueset"))
	            v = new ValueSetValidator(cls.toLowerCase(),param,rc);
   	    else {
	            log.error("unknown validator, cls="+cls);
	            throw new CodecException("unknown validator, cls="+cls);
	    }
	    cache.put(key,v);
	    return v;
    }
	
	protected String cls;
	protected String param;
	protected int returnCode;
	
	abstract public int validate(Object a);

	public Validator(String cls,String param,int returnCode) {
		this.cls = cls;
		this.param = param;
		this.returnCode = returnCode;
	}

}


class RequireValidator extends Validator {

	RequireValidator(String cls, String param, int returnCode) {
		super(cls,param,returnCode);
	}
	
	public int validate(Object a) {
        if( a == null )
            return returnCode;
        
        if( a instanceof String ) {
        	String s = (String)a;
            if( s.equals(""))
                return returnCode;
            return 0;
        }
        else if( a instanceof List ) {
        	List list = (List)a;
            if( list.size() == 0 )
                return returnCode;
            return 0;
        }
        return 0;
    }

}

class NumberRangeValidator extends Validator {

    int min = Integer.MIN_VALUE;
    int max = Integer.MAX_VALUE;

    NumberRangeValidator(String cls, String param, int returnCode) {
		super(cls,param,returnCode);
		init();
	}
    
    void init() {
        if( param == null || param.equals("") ) 
            throw new CodecException("number range validator param is not valid, param is empty");
        int p = param.indexOf(",");
        if( p < 0 )
            throw new CodecException("number range validator param is not valid, param="+param);
        String v1 = param.substring(0,p);
        String v2 = param.substring(p+1);
        if( !v1.equals("") )
            min = Integer.parseInt(v1);
        if( !v2.equals("") )
            max = Integer.parseInt(v2);
        if( max < min )
            throw new CodecException("number range validator param is not valid, param="+param);
    }

    public int validate(Object a) {
        if( a == null )
            return returnCode;
        
        if( a instanceof String ) {
        	String s = (String)a;
            if( s.equals(""))
                return returnCode;

            try {
                int i = Integer.parseInt(s);
                if( i < min || i > max ) 
                    return returnCode;
                return 0;
            } catch(Exception e) {
                return returnCode;
            }
        }
        else if( a instanceof Integer ) {
        	int i = (Integer)a;
        	if( i < min || i > max ) 
                return returnCode;
        	return 0;
        }
        return returnCode;
    }

}

class LengthRangeValidator extends Validator {

    int min = Integer.MIN_VALUE;
    int max = Integer.MAX_VALUE;

	LengthRangeValidator(String cls, String param, int returnCode) {
		super(cls,param,returnCode);
		init();
	}

    void init() {
    	if( param == null || param.equals("") ) 
            throw new CodecException("number range validator param is not valid, param is empty");
        int p = param.indexOf(",");
        if( p < 0 )
            throw new CodecException("number range validator param is not valid, param="+param);
        String v1 = param.substring(0,p);
        String v2 = param.substring(p+1);
        if( !v1.equals("") )
            min = Integer.parseInt(v1);
        if( !v2.equals("") )
            max = Integer.parseInt(v2);
        if( max < min )
            throw new CodecException("number range validator param is not valid, param="+param);
    }

    public int validate(Object a) {
    	if( a == null )
            return returnCode;
        
        if( a instanceof String ) {
        	String s = (String)a;
            if( s.equals(""))
                return returnCode;
            int len = s.length();
	        if( len < min || len > max ) 
	            return returnCode;
	        return 0;
        }
        else if( a instanceof Integer ) {
        	int len = a.toString().length();
        	if( len < min || len > max ) 
                return returnCode;
        	return 0;
        }
        return returnCode;
    }

}
class TimeRangeValidator extends Validator {

    String min = "1970-01-01 00:00:00";
    String max = "2099-01-01 00:00:00";

    TimeRangeValidator(String cls, String param, int returnCode) {
		super(cls,param,returnCode);
		init();
	}

    void init() {
        if( param == null || param.equals("") ) 
            throw new CodecException("time range validator param is not valid, param is empty");
        int p = param.indexOf(",");
        if( p < 0 )
            throw new CodecException("time range validator param is not valid, param="+param);
        String v1 = param.substring(0,p);
        String v2 = param.substring(p+1);
        if( !v1.equals("") )
            min = v1;
        if( !v2.equals("") )
            max = v2;
        if( max.compareTo(min) < 0 )
            throw new CodecException("time range validator param is not valid, param="+param);
    }

    public int validate(Object a) {
    	if( a == null )
            return returnCode;
        
        if( a instanceof String ) {
        	String s = (String)a;
            if( s.equals(""))
                return returnCode;
            if( s.compareTo(min) < 0 || s.compareTo(max) > 0 ) 
                return returnCode;
            return 0;
        }
        return returnCode;
    }

}
class ValueSetValidator  extends Validator {

	HashSet<String> set = new HashSet<String>();

	ValueSetValidator(String cls, String param, int returnCode) {
		super(cls,param,returnCode);
		init();
	}

    void init() {
        if( param == null || param.equals("") ) 
            throw new CodecException("value set validator param is not valid, param is empty");
        String[] ss = param.split("\\|");
        for( String s: ss ) set.add(s);
    }

    public int validate(Object a) {
    	if( a == null )
            return returnCode;
        
        if( a instanceof String || a instanceof Integer) {
        	String s = a.toString();
            if( s.equals(""))
                return returnCode;
            if( !set.contains(s) ) 
                return returnCode;
	        return 0;
        }
        return returnCode;
    }

}
class RegexValidator extends Validator {

	Pattern p;

    RegexValidator(String cls, String param, int returnCode) {
		super(cls,param,returnCode);
		init();
	}

    void init() {
        if( param == null || param.equals("") ) 
            throw new CodecException("regex validator param is not valid, param is empty");
        p = Pattern.compile(param);
    }

    public int validate(Object a) {
    	if( a == null )
            return returnCode;
        
        if( a instanceof String || a instanceof Integer) {
        	String s = a.toString();
            if( s.equals(""))
                return returnCode;
            if( !p.matcher(s).matches() )  {
                return returnCode;
            }
	        return 0;
        }
        return returnCode;
    }

}

