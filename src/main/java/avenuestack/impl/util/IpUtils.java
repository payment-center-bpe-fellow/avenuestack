package avenuestack.impl.util;

import java.net.InetAddress;
import java.net.NetworkInterface;
import java.util.ArrayList;
import java.util.Enumeration;

import org.apache.commons.lang3.StringUtils;

public class IpUtils {

    static ArrayList<String> localips  = new ArrayList<String>();
    static ArrayList<String> netips = new ArrayList<String>();

    static {
    	loadIps();
    }

    static void loadIps() {

        try {
        	Enumeration<NetworkInterface> netInterfaces = NetworkInterface.getNetworkInterfaces();
            while (netInterfaces.hasMoreElements()) {
            	Enumeration<InetAddress> address = netInterfaces.nextElement().getInetAddresses();
                while (address.hasMoreElements()) {
                	InetAddress ip = address.nextElement();

                    if (!ip.isSiteLocalAddress() && !ip.isLoopbackAddress() && ip.getHostAddress().indexOf(":") == -1) {
                        netips.add( ip.getHostAddress() );
                    } else if (ip.isSiteLocalAddress() && !ip.isLoopbackAddress() && ip.getHostAddress().indexOf(":") == -1) {
                        localips.add( ip.getHostAddress() );
                    }
                }
            }
        } catch(Exception e) {
        }

        //println("localips="+localips.mkString(","))
        //println("netips="+netips.mkString(","))
    }

    static String localIp0() {

        try {
        	InetAddress addr = InetAddress.getLocalHost();
            return addr.getHostAddress();
        } catch(Exception e) {
            return "127.0.0.1";
        }

    }

    static String localIp() {

        String ip0 = localIp0();

        if ( localips.size() > 0  ) {
            if( localips.contains(ip0)) return ip0;
            return localips.get(0) ;
        }

        if ( netips.size() > 0  ) {
            if( netips.contains(ip0)) return ip0;
            return netips.get(0) ;
        }

        return ip0;
    }

    static String serverId() {
        String s = localIp();
        String[] ss = s.split("\\.");
        //String t = "%03d%03d".format(ss[2].toInt,ss[3].toInt);
        String t = StringUtils.leftPad(ss[2],3,'0')+StringUtils.leftPad(ss[3],3,'0');
        return t;
    }


}


