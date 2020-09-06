package cn.edu.tsinghua.au.bioinfo.btree;

import junit.framework.TestCase;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;

import java.util.List;

public class AppConfigTest extends TestCase {

    public void testMysql(){
        AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext(AppConfig.class);
        MysqlDb mysqlDb = context.getBean(MysqlDb.class);
        mysqlDb.addRow(41,
                new String[]{"label1", "label2", "label3", "label4"}, List.of("K cell", "CD12+", "x cell", "2", "34"));
        mysqlDb.head();
        System.out.println("mysqlDb.getBiggestId() = " + mysqlDb.getBiggestId());
        System.out.println("mysqlDb.getAllIds() = " + mysqlDb.getAllIds());
        context.close();
    }

    public void testBtree(){
        AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext(AppConfig.class);
        BtreeSearcher bts = context.getBean(BtreeSearcher.class);
        bts.getColumnInfo();
        context.close();
    }
}