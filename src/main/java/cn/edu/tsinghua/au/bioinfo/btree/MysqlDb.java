package cn.edu.tsinghua.au.bioinfo.btree;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

import java.sql.ResultSet;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * @author panjx
 */
@Component
public class MysqlDb {

    JdbcTemplate jdbcTemplate;


    public MysqlDb(@Autowired JdbcTemplate jdbcT) {
        this.jdbcTemplate = jdbcT;
    }

    /**
     * 添加行
     *
     * @param cellid       行编号
     * @param columnNames  列名
     * @param columnValues 对应列值
     */
    @LoggingPoint("add row")
    public void addRow(Long cellid,
                       String[] columnNames,
                       List<Object> columnValues) {
        String sql = "SELECT * FROM hcaddb.new_table";
        jdbcTemplate.query(connection -> connection.prepareStatement(sql, ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_UPDATABLE),
                rs -> {
                    rs.moveToInsertRow();
                    rs.updateLong("cellid", cellid);
                    for (int i = 0; i < columnNames.length; i++) {
                        rs.updateObject(columnNames[i], columnValues.get(i));
                    }
                    rs.insertRow();
                    return null;
                }
        );
    }

    /**
     * 获取当前所有id
     *
     * @return Set集合
     */
    public Set<Long> getAllIds() {
        String sql = "SELECT * FROM hcaddb.new_table";
        var result = new HashSet<Long>();
        var rows = jdbcTemplate.queryForList(sql);
        for (var map : rows) {
            result.add((Long) map.get("cellid"));
        }
        return result;
    }

    /**
     * 搜索字符串一一匹配的行, 范围是全部ID
     *
     * @param columnNames  列名
     * @param columnValues 列值
     * @return 所有匹配的id组成的Set
     */
    @LoggingPoint("queryByStringEqual")
    public Set<Long> queryByStringEqual(String[] columnNames,
                                        String[] columnValues) {
        return queryByStringEqual(columnNames, columnValues, getAllIds());
    }

    /**
     * 搜索字符串一一匹配的行, 范围是指定ID
     *
     * @param columnNames  列名
     * @param columnValues 列值
     * @return 所有匹配的id组成的Set
     */
    @LoggingPoint("queryByStringEqual")
    public Set<Long> queryByStringEqual(String[] columnNames,
                                        String[] columnValues,
                                        Set<Long> candidate) {
        Set<Long> result = new HashSet<>();
        candidate.forEach(cellid ->
        {
            String sql = "SELECT * FROM hcaddb.new_table WHERE cellid = ?";
            jdbcTemplate.query(sql, (ResultSet rs) -> {
                // 查看当前行是否匹配所有值
                boolean match = true;
                if (rs.next()) {
                    for (int i = 0; i < columnNames.length; i++) {
                        if (!columnValues[i].equals(rs.getString(columnNames[i]))) {
                            match = false;
                            break;
                        }
                    }
                }
                if (match) {
                    result.add(cellid);
                }
            }, cellid);
        });
        return result;
    }
}
