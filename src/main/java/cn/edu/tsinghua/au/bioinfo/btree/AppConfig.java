package cn.edu.tsinghua.au.bioinfo.btree;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.*;
import org.springframework.jdbc.core.JdbcTemplate;

import javax.sql.DataSource;

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
    public JdbcTemplate createJdbcTemplate(@Autowired DataSource dataSource) {
        return new JdbcTemplate(dataSource);
    }

    @Bean
    public BtreeInitializer createBtree() {
        return new BtreeInitializer(dirPath);
    }

    @Bean
    public static Logger getLog() {
        return LoggerFactory.getLogger(AppConfig.class);
    }
}
