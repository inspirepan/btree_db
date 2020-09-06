package cn.edu.tsinghua.au.bioinfo.btree;

import btree4j.Value;
import btree4j.indexer.BasicIndexQuery;
import junit.framework.TestCase;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;

import java.util.List;

public class AppConfigTest extends TestCase {

    public void testMysql() {
        ApplicationContext context = new AnnotationConfigApplicationContext(AppConfig.class);
        MysqlDb mysqlDb = context.getBean(MysqlDb.class);
        mysqlDb.addRow(45,
                new String[]{"label1", "label2", "label3", "label4"}, List.of("K cell", "CD12+", "x cell", "2", "34"));
        mysqlDb.head();
        System.out.println("mysqlDb.getBiggestId() = " + mysqlDb.getBiggestId());
        System.out.println("mysqlDb.getAllIds() = " + mysqlDb.getAllIds());
        System.out.println(mysqlDb.queryByStringEqual(new String[]{"12"}, new String[]{"CD19+"}));
        mysqlDb.removeRow(44);
    }

    public void testBtreeInitializer() {
        ApplicationContext context = new AnnotationConfigApplicationContext(AppConfig.class);
        BtreeInitializer bti = context.getBean(BtreeInitializer.class);
        System.out.println(bti.getBtreeMap());
    }

    public void testBtreeSearcher() {
        ApplicationContext context = new AnnotationConfigApplicationContext(AppConfig.class);
        IBtreeSearcher bts = context.getBean(IBtreeSearcher.class);
        try {
            bts.getColumnInfo();
            bts.addColumn("label4");
            bts.getColumnInfo();
            bts.insert(3, new String[]{"label1", "label2", "label3", "label4"}, new double[]{1, 2, 3, 4}, 4);
            BasicIndexQuery.IndexConditionEQ condition = new BasicIndexQuery.IndexConditionEQ(new Value(1));
            BasicIndexQuery[] conditions = new BasicIndexQuery[]{condition};
            System.out.println(bts.rangeSearch(new String[]{"label1"}, conditions, 1));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}