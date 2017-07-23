package avenuestack.impl.avenue;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;

import org.dom4j.Element;
import org.dom4j.io.SAXReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TlvCodecs {

	static Logger log = LoggerFactory.getLogger(TlvCodecs.class);
	
	static String CLASSPATH_PREFIX = "classpath:";
	
	HashMap<Integer,TlvCodec> codecs_id = new HashMap<Integer,TlvCodec>();
	HashMap<String,TlvCodec> codecs_names = new HashMap<String,TlvCodec>();
	HashMap<Integer,Integer> version_map = new HashMap<Integer,Integer>();
			
	public TlvCodecs(String dir) throws Exception {
		init(getAllXmlFiles(dir));
	}

	public TlvCodecs(ArrayList<String> xmlfiles) throws Exception {
		init(xmlfiles);
	}
	
    public ArrayList<String> getAllXmlFiles(String dir) throws Exception {

        ArrayList<String> xmls = new ArrayList<String>();
        ArrayList<String> xmls_sub = new ArrayList<String>();
        
        File[] files = new File(dir).listFiles();
        for(File f : files) {
        	if(f.getPath().endsWith(".xml"))
        		xmls.add(f.getPath());
        	if(f.isDirectory()) {
        		File[] subfiles = f.listFiles();
        		for(File f2 : subfiles) {
                	if(f2.getPath().endsWith(".xml"))
                		xmls_sub.add(f2.getPath());
        		}
        	}
        }

        HashSet<String> nameset = new HashSet<String>();
        for( String f : xmls ) {
        	File file = new File(f);
            nameset.add(file.getName());
        }
        for( String f : xmls_sub ) {
        	File file = new File(f);
            if( nameset.contains(file.getName()) )
                throw new RuntimeException("avenue config filename duplicated, file="+f);
        }

        ArrayList<String> allxmls = new ArrayList<String>();
        for(String s:xmls) allxmls.add(s);
        for(String s:xmls_sub) allxmls.add(s);
        return allxmls;
    }
    
    public void init(ArrayList<String> allxmls) throws Exception {

        for(String f : allxmls ) {
            try {
        		SAXReader saxReader = new SAXReader();
        		saxReader.setEncoding("UTF-8");
        		
        		InputStream in;
        		if( f.startsWith(CLASSPATH_PREFIX))
        			in = TlvCodecs.class.getResourceAsStream(f.substring(CLASSPATH_PREFIX.length()));
        		else
        			in = new FileInputStream(f);
        		
        		Element cfgXml = saxReader.read(in).getRootElement();
        		in.close();

                int id = Integer.parseInt(cfgXml.attributeValue("id"));
                String name = cfgXml.attributeValue("name").toLowerCase();

                TlvCodec codec = new TlvCodec(f);

                if( codecs_id.get(id) != null ) {
                    throw new RuntimeException("service id duplicated serviceId="+id);
                }
                if( codecs_names.get(name) != null ) {
                    throw new RuntimeException("service name duplicated name="+name);
                }

                codecs_id.put(id,codec);
                codecs_names.put(name,codec);
                
                parseVersion(id, cfgXml);
                
            } catch(Exception e) {
                    log.error("load xml failed, f="+f);
                    throw e;
            }
        }
    }
    
	public TlvCodec findTlvCodec(int serviceId) {
        return codecs_id.get(serviceId);
    }

	public String serviceNameToId(String service) {
        String[] ss = service.toLowerCase().split("\\.");
        if( ss.length != 2 ) {
            return "0.0";
        }
        TlvCodec codec = codecs_names.get(ss[0]);
        if( codec == null ) {
            return "0.0";
        }
        int msgId= codec.msgNameToId(ss[1]);
        if( msgId == 0 ) {
            return "0.0";
        }

        return codec.serviceId+"."+msgId;
    }

    public String serviceIdToName(int serviceId,int msgId) {
    	TlvCodec codec = findTlvCodec(serviceId);
        if( codec == null ) {
            return "service"+serviceId+"."+"msg"+msgId;
        }

        String msgName = codec.msgIdToName(msgId);
        if( msgName == null ) {
            return codec.serviceName+"."+"msg"+msgId;
        }

        return codec.serviceName +"." + msgName;
    }

    public List<Integer> allServiceIds() {
    	List<Integer> list = new ArrayList<Integer>();
        for(int i : codecs_id.keySet())
        	list.add(i);
        return list;
    }
    public void parseVersion(int serviceId, Element cfgXml) {
        String version = cfgXml.attributeValue("version","");
        if (!version.equals(""))
            version_map.put(serviceId, Integer.parseInt(version));
        else
            version_map.put(serviceId, 1);
    }

    public int version(int serviceId)  {
        Integer v = version_map.get(serviceId);
        if( v == null ) return 1;
        return v;
    }
}

