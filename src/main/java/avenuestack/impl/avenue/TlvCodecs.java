package avenuestack.impl.avenue;

import java.io.File;
import java.io.FileInputStream;
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
	
	HashMap<Integer,TlvCodec> codecs_id = new HashMap<Integer,TlvCodec>();
	HashMap<String,TlvCodec> codecs_names = new HashMap<String,TlvCodec>();
	
	private String dir;
	
	public TlvCodecs(String dir) throws Exception {
		this.dir = dir;
		init();
	}

	// todo load file from classpath
	
    public void init() throws Exception {

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

        for(String f : allxmls ) {

            try {
            	
        		SAXReader saxReader = new SAXReader();
        		saxReader.setEncoding("UTF-8");
        		FileInputStream in = new FileInputStream(f);
        		Element cfgXml = saxReader.read(in).getRootElement();
        		in.close();

                String name = cfgXml.attributeValue("name").toLowerCase();
                int id = Integer.parseInt(cfgXml.attributeValue("id"));

                TlvCodec codec = new TlvCodec(f);

                if( codecs_id.get(id) != null ) {
                    throw new RuntimeException("service id duplicated serviceId="+id);
                }
                if( codecs_names.get(name) != null ) {
                    throw new RuntimeException("service name duplicated name="+name);
                }

                codecs_id.put(id,codec);
                codecs_names.put(name,codec);
            } catch(Exception e) {
                    log.error("load xml failed, f="+f);
                    throw e;
            }
        }

        log.info("validator size="+Validator.cache.size());
        log.info("encoder size="+Encoder.cache.size());
        log.info("tlvfieldinfo size="+TlvFieldInfo.cache.size());
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

}

