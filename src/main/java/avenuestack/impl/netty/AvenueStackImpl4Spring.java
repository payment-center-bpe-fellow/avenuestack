package avenuestack.impl.netty;

import org.apache.commons.lang3.StringUtils;
import org.springframework.core.env.Environment;

public class AvenueStackImpl4Spring extends AvenueStackImpl {


    private Environment env;

    public AvenueStackImpl4Spring() throws Exception {
        super();
        profile = System.getProperty("spring.profiles.active");
        confDir = CLASSPATH_PREFIX;
    }

    public AvenueStackImpl4Spring(String absoluteConfPath) throws Exception {
        super();
        profile = System.getProperty("spring.profiles.active");
        confDir = StringUtils.isNotBlank(absoluteConfPath) ? absoluteConfPath : CLASSPATH_PREFIX;
    }

    String getParameter(String key) {
        String v = super.getParameter(key);
        if (v != null) return v;
        if (env == null) return null;
        return env.getProperty(key);
    }

    public Environment getEnv() {
        return env;
    }

    public void setEnv(Environment env) {
        this.env = env;
    }

    public void setConfDir(String confDirPath) {
        this.confDir = confDirPath;
    }

}
