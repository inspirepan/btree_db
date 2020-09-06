package cn.edu.tsinghua.au.bioinfo.btree;

import org.apache.commons.logging.Log;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.annotation.Before;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

/**
 * @author panjx
 */
@Aspect
@Component
public class Logging {
    final Log log;

    public Logging(@Autowired Log log) {
        this.log = log;
    }

    @Before("@annotation(loggingPoint)")
    public void mysqlLogging(LoggingPoint loggingPoint) {
        log.error("[mysql] " + loggingPoint.value());
        System.out.println("[mysql] " + loggingPoint.value());
    }
}
