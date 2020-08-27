package cn.edu.tsinghua.au.bioinfo.btree;

import org.aspectj.lang.annotation.After;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.annotation.Before;
import org.springframework.stereotype.Component;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.logging.Log;

/**
 * @author panjx
 */
@Aspect
@Component
public class Logging {
    Log log = LogFactory.getLog(Logging.class);

    @Before("@annotation(loggingPoint)")
    public void mysqlLogging(LoggingPoint loggingPoint) {
        log.info("[mysql] start " + loggingPoint.value());
    }

}
