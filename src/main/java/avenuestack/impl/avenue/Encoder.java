package avenuestack.impl.avenue;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/*
encoder定义为：

1、	encoder系列标签可以出现在<type>内 或者请求/响应中的<field>内, 或者struct里的<field>内, 请求响应中的配置优先于type上的配置
2、	encoder系列标签有2个：encoder、encoderParam分别表示编码名、编码参数
3、	encoder对请求、响应均有效
4、	拟实现的Encoder有：

encoder	            encoderParam	        参数说明	    实现说明

NormalEncoder	    A,b|c,d|<,&lt	        |是大分割符，逗号是小分隔符，代表将A转义为b,将c转义为d, |,\三个字符实现为关键字，要输入实际这三个字符使用\转义，比如\|   \,  \\
HtmlEncoder	        无	                    无	        基于NormalEncoder实现，等价于： &,&amp;|<,&lt;|>,&gt;|",&quot;|',&#x27;|/,&#x2f;
HtmlFilter	        无	                    无	        基于NormalEncoder实现，等价于： &,|<,|>,|",|',|/,|\\,
NocaseEncoder       无	                    无	        不区分大小写的NormalEncoder编码转换
AttackFilter        无	                    无	        基于NocaseEncoder,等价于： script,|exec,|select,|update,|delete,|insert,|create,|alter,|drop,|truncate,|&,|<,|>,|",|',|/,|\\,
*/

public abstract class Encoder {

	static Logger log = LoggerFactory.getLogger(Encoder.class);
	
	static HashMap<String,Encoder> cache = new HashMap<String,Encoder>();
	static String htmlParam = "&,&amp;|<,&lt;|>,&gt;|\",&quot;|',&#x27;|/,&#x2f;";
	static String htmlFilterParam = "&,|<,|>,|\",|',|/,|\\\\,";
	static String attackFilterParam = "script,|exec,|select,|update,|delete,|insert,|create,|alter,|drop,|truncate,|&,|<,|>,|\",|',|/,|\\\\,";

	static public Encoder getEncoder(String cls, String param) {
        if( cls == null || cls.equals("") ) return null;

        String key = cls.toLowerCase() + ":" + param;
        Encoder encoder = cache.get(key);
        if( encoder != null ) return encoder;

        cls = cls.toLowerCase();
	    if( cls.equals("escapeencoder") || cls.equals("dummyencoder"))
            encoder = new DummyEncoder(cls,param);
	    else if( cls.equals("normalencoder") )
	    	encoder = new NormalEncoder(cls,param);
	    else if( cls.equals("htmlencoder") )
	    	encoder = new HtmlEncoder(cls,htmlParam);
        else if( cls.equals("htmlfilter") )
        	encoder = new NormalEncoder(cls,htmlFilterParam);
        else if( cls.equals("nocaseencoder") )
        	encoder = new NoCaseEncoder(cls,param);
        else if( cls.equals("attackfilter") )
            encoder = new NoCaseEncoder(cls,attackFilterParam);
        else if( cls.equals("maskencoder") )
            encoder = new MaskEncoder(cls,param);
        else {
                log.error("unknown encoder, cls="+cls);
                throw new CodecException("unknown encoder, cls="+cls);
        }
        cache.put(key,encoder);
        return encoder;
    }

	protected String cls;
	protected String param;

	abstract public Object encode(Object a);

	public Encoder(String cls,String param) {
		this.cls = cls;
		this.param = param;
	}

}

class DummyEncoder extends Encoder {
	public DummyEncoder(String cls,String param) {
		super(cls,param);
	}
	public Object encode(Object a) {
		return a;
	}
}

class NormalEncoder extends Encoder {

	static Logger log = LoggerFactory.getLogger(NormalEncoder.class);

    String[] js = new String[] { "\\\\","\\|","\\," };
    String[] ks = new String[] { new String(new byte[] {1} ), new String(new byte[] {2} ), new String(new byte[] {3} ) };
    String[] ls = new String[] { "\\","|","," };

    String[] p1 = null;
    String[] p2 = null;

	public NormalEncoder(String cls,String param) {
		super(cls,param);
		init();
	}

    void init() {
        if( param == null || param == "" ) return;
        String t = j2k(param);
        String[] ss = t.split("\\|");
        ArrayList<String> p1b = new ArrayList<String>();
        ArrayList<String> p2b = new ArrayList<String>();

        for( String s: ss ) {
            int p = s.indexOf(",");
            if( p <= 0 ) throw new CodecException("encoder param not valid, comma not found, param="+param);
            String k = k2j( s.substring(0,p) );
            String v = k2j( s.substring(p+1) );
            p1b.add(k);
            p2b.add(v);
        }
        p1 = p1b.toArray(new String[0]);
        p2 = p2b.toArray(new String[0]);
    }

    String j2k(String s) {
        return StringUtils.replaceEach(s,js,ks);
    }
    String k2j(String s) {
    	return StringUtils.replaceEach(s,ks,ls);
    }
    String encodeString(String s) {
        if( s == null ) return s;
        return StringUtils.replaceEach(s,p1,p2);
    }

    int encodeInt(int i) {
        try {
            return Integer.parseInt( encodeString(String.valueOf(i)) );
        } catch(Exception e) {
            log.error("encoder int error, i="+i+",cls="+cls+",param="+param);
            return i;
        }
    }

    public Object encode(Object a) {
    	if( a == null ) return a;
    	if( a instanceof String ) {
    		return encodeString((String)a);
    	}
    	if( a instanceof Integer ) {
    		return encodeInt((Integer)a);
    	}
        return a;
    }
}

class NoCaseEncoder extends NormalEncoder {

	public NoCaseEncoder(String cls,String param) {
		super(cls,param);
		init();
	}

    void init() {
        super.init();
        for(int i=0;i<p1.length;++i) {
			p1[i] = p1[i].toLowerCase();
		}
    }

    String encodeString(String s) {
        if( s == null ) return s;
        StringBuilder b = new StringBuilder(s) ;
        int i = 0;
        while( i < p1.length ) {
            replace(b,p1[i],p2[i]);
            i += 1;
        }
        return b.toString();
    }

    void replace(StringBuilder b,String s,String d) {
        int slen = s.length();
        int dlen = d.length();
        int p = org.apache.commons.lang3.StringUtils.indexOfIgnoreCase(b,s,0);
        while( p >= 0 ) {
            b.replace(p,p+slen,d);
            p = org.apache.commons.lang3.StringUtils.indexOfIgnoreCase(b,s,p+dlen);
        }
    }
}

// 注意: 
// NormalEncoder类在处理源和目标有重叠的情况，多次encoder会有问题
// HtmlEncoder对这个做了特殊处理，多次encoder不会引起问题
class HtmlEncoder extends NormalEncoder {

    String[] p3 = null;

	public HtmlEncoder(String cls,String param) {
		super(cls,param);
		super.init();
		
		p3 = new String[p2.length];
		for(int i=0;i<p3.length;++i) {
			p3[i] = new String( new byte[]{(byte)(i+1)});
		}
	}

	String encodeString(String s) {
        if( s == null ) return s;
        String t = StringUtils.replaceEach(s,p2,p3);
        t = super.encodeString(t);
        t = StringUtils.replaceEach(t,p3,p2);
        return t;
    }

}

class MaskEncoder extends Encoder {

    String commonRegexStr = "^common:([0-9]+):([0-9]+)$";
    Pattern commonRegex = null;
    String tp;
    int start = 3;
    int end = 4;

    public MaskEncoder(String cls,String param) {
		super(cls,param);
		commonRegex = Pattern.compile(commonRegexStr);
        tp = param == null ? "" : param.toLowerCase();
		init();
	}

    public void init() {
        if( tp.equals("phone") ||
    		tp.equals("email") ||
    		tp.equals("account") ) return;

        Matcher m = commonRegex.matcher(tp);
        if( m.matches() )  {
        	tp = "common";
            start = Integer.parseInt(m.group(1));
            end = Integer.parseInt(m.group(2));
            return;
        }        

        log.error("unknown encoder param, param="+param);
        throw new CodecException("unknown encoder param, param="+param);
    }

    public Object encode(Object a) {
    	if( a == null ) return a;
    	if( a instanceof String ) {
    		return encodeString((String)a);
    	}
    	if( a instanceof Integer ) {
    		return (Integer)a;
    	}
        return a;        
    }
    
    String encodeString(String s) {
        if( s == null ) return s;

        if( tp.equals("phone") ) return getCommonMaskForPhone(s);
  		if( tp.equals("email") ) return getCommonMaskForEmail(s);
  		if( tp.equals("account") ) return getCommonMaskForCustomAccount(s);
        if( tp.equals("common") ) return getCommonMask(s,start,end);
        return s;
    }

    String getCommonMaskForPhone(String phone) {
        return getCommonMask(phone,3,4);
    }
    String getCommonMaskForEmail(String s) {
        if( s == null ) return null;
        if( s.length() <= 2 ) return s;
        int p = s.indexOf("@");
        String name = s.substring(0,p);
        String suffix = s.substring(p);
        if( name.length() <= 2 ) return s;
        switch (name.length()) {
            case 3: return getCommonMask(name,1,1)+suffix;
            case 4: return getCommonMask(name,2,1)+suffix;
            case 5: return getCommonMask(name,2,2)+suffix;
            case 6: return getCommonMask(name,3,2)+suffix;
            default: return getCommonMask(name,3,3)+suffix;
        }
    }
    String getCommonMaskForCustomAccount(String s) {
        if( s == null ) return null;
        if( s.length() <= 2 ) return s;
        switch(s.length()) {
            case 3 : return getCommonMask(s,1,1);
            case 4 : return getCommonMask(s,2,1);
            case 5 : return getCommonMask(s,2,2);
            case 6 : return getCommonMask(s,3,2);
            default: return getCommonMask(s,3,3);
        }
    }
    String getCommonMask(String s,int start,int end) {
        if( s == null ) return null;
        if( s.length() <= start+end ) return s;
        return s.substring(0,start) + "****" + s.substring(s.length() - end);
    }
}


