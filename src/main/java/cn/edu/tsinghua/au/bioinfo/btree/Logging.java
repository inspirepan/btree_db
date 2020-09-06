package cn.edu.tsinghua.au.bioinfo.btree;

import org.slf4j.Logger;
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
    final Logger log;

    public Logging(@Autowired Logger log) {
        this.log = log;
    }

    @Before("@annotation(loggingPoint)")
    public void mysqlLogging(LoggingPoint loggingPoint) {
        log.info(loggingPoint.value());
    }
}
