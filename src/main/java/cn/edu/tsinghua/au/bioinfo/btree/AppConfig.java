package cn.edu.tsinghua.au.bioinfo.btree;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.*;
import org.springframework.jdbc.core.JdbcTemplate;

import javax.sql.DataSource;
import java.util.List;

/**
 * @author panjx
 */

@Configuration
@ComponentScan
@PropertySource("/btree.properties")
@EnableAspectJAutoProxy
public class AppConfig {
    @Value("${jdbc.url}")
    String jdbcUrl;

    @Value("${jdbc.username}")
    String jdbcUsername;

    @Value("${jdbc.password}")
    String jdbcPassword;

    @Value("${btree.path}")
    String dirPath;

    @Bean
    DataSource createDataSource() {
        HikariConfig config = new HikariConfig();
        config.setJdbcUrl(jdbcUrl);
        config.setUsername(jdbcUsername);
        config.setPassword(jdbcPassword);
        config.addDataSourceProperty("autoCommit", "true");
        config.addDataSourceProperty("connectionTimeout", "5");
        config.addDataSourceProperty("idleTimeout", "60");
        return new HikariDataSource(config);
    }

    @Bean
    JdbcTemplate createJdbcTemplate(@Autowired DataSource dataSource) {
        return new JdbcTemplate(dataSource);
    }

    @Bean
    BtreeDb createBtree() {
        return new BtreeDb(dirPath);
    }

    public static void main(String[] args) {
        ApplicationContext context = new AnnotationConfigApplicationContext(AppConfig.class);
        MysqlDb mysqlDb = context.getBean(MysqlDb.class);

        mysqlDb.addRow(41,
                new String[]{"label1", "label2", "label3", "label4"}, List.of("K cell", "CD12+", "x cell", "2", "34"));
        mysqlDb.head();
        System.out.println(mysqlDb.getBiggestId());
    }
}
