package cn.edu.tsinghua.au.bioinfo.btree;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

import java.sql.ResultSet;
import java.util.*;

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
     * 获取当前所有id
     *
     * @return Set集合
     */
    public Set<Long> getAllIds() {
        String sql = "SELECT cellid FROM hcaddb.new_table";
        var rows = jdbcTemplate.queryForList(sql, Long.TYPE);
        return new HashSet<>(rows);
    }

    public Long getBiggestId() {
        String sql = "SELECT cellid FROM hcaddb.new_table ORDER BY cellid DESC LIMIT 1 OFFSET 0";
        return jdbcTemplate.queryForObject(sql,
                (rs, rowNum) -> rs.getLong(1));
    }

    /**
     * 打印出前5行的所有内容
     */
    @LoggingPoint("head")
    public void head() {
        String sql = "SELECT * FROM hcaddb.new_table LIMIT 5 OFFSET 0";
        jdbcTemplate.query(sql,
                rs -> {
                    int count = rs.getMetaData().getColumnCount();
                    System.out.println("-".repeat(count * 14));
                    for (int i = 1; i <= count; i++) {
                        System.out.format("%-14s", rs.getMetaData().getColumnName(i));
                    }
                    System.out.println();
                    while (rs.next()) {
                        for (int i = 1; i <= count; i++) {
                            System.out.format("%-14s", rs.getString(i));
                        }
                        System.out.println();
                    }
                    System.out.println("-".repeat(count * 14));
                    return null;
                });
    }

    /**
     * 搜索字符串一一匹配的行, 范围是全部ID
     *
     * @param columnNames  列名
     * @param columnValues 列值
     * @return 所有匹配的id组成的Set
     */
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
            jdbcTemplate.query(sql,
                    ps -> ps.setLong(1, cellid),
                    rs -> {
                        // 查看当前行是否匹配所有值
                        boolean match = true;
                        for (int i = 0; i < Math.min(columnNames.length, columnValues.length); i++) {
                            if (!columnValues[i].equals(rs.getString(columnNames[i]))) {
                                match = false;
                                break;
                            }
                        }
                        if (match) {
                            result.add(cellid);
                        }
                    });
        });
        return result;
    }

    /**
     * 数据库中是否已经包含给定cellid
     *
     * @param cellid 查询的cellid
     * @return 是否包含
     */
    @LoggingPoint("containsId")
    public boolean containsId(Long cellid) {
        String sql = "SELECT COUNT(*) FROM hcaddb.new_table WHERE cellid = ?";
        Long lineNum = jdbcTemplate.queryForObject(sql, (rs, rowNum) -> rs.getLong(1), cellid);
        return lineNum != null && (lineNum == 1);
    }

    @LoggingPoint("containsId")
    public boolean containsId(int cellid) {
        return containsId((long) cellid);
    }


    /**
     * 设值函数，如果cellid不存在，就不进行任何操作
     *
     * @param cellid       行编号
     * @param columnNames  列名，支持只选择部分列
     * @param columnValues 对应列值
     * @return 成功则返回true，如果cellid不存在，返回false
     */
    @LoggingPoint("updateRow")
    public boolean updateRow(Long cellid,
                             String[] columnNames,
                             List<Object> columnValues) {
        return setRow(cellid, columnNames, columnValues);
    }

    @LoggingPoint("updateRow")
    public boolean updateRow(int cellid,
                             String[] columnNames,
                             List<Object> columnValues) {
        return setRow((long) cellid, columnNames, columnValues);
    }


    /**
     * updateRow和addRow的实现函数，为了在LogPoint中区分，从updateRow中拆分出来并设为private。
     *
     * @param cellid       行编号
     * @param columnNames  列名
     * @param columnValues 对应列值
     * @return 成功则返回true，如果cellid不存在，返回false
     */
    private boolean setRow(Long cellid,
                           String[] columnNames,
                           List<Object> columnValues) {
        if (!containsId(cellid)) {
            return false;
        }
        String sql = "SELECT * FROM hcaddb.new_table WHERE cellid = ?";
        Boolean result = jdbcTemplate.query(
                connection -> connection.prepareStatement(sql, ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_UPDATABLE),
                ps -> ps.setLong(1, cellid),
                rs -> {
                    while (rs.next()) {
                        if (cellid == rs.getLong("cellid")) {
                            for (int i = 0; i < Math.min(columnNames.length, columnValues.size()); i++) {
                                rs.updateObject(columnNames[i], columnValues.get(i));
                            }
                            rs.updateRow();
                            return true;
                        }
                    }
                    return false;
                }
        );
        return result != null && result;
    }


    /**
     * 添加行
     *
     * @param cellid       行编号
     * @param columnNames  列名，支持只选择部分列,这样的话其他列会设为默认值或者null
     * @param columnValues 对应列值
     */
    @LoggingPoint("addRow")
    public void addRow(Long cellid,
                       String[] columnNames,
                       List<Object> columnValues) {
        if (containsId(cellid)) {
            setRow(cellid, columnNames, columnValues);
        } else {
            String sql = "SELECT * FROM hcaddb.new_table";
            // 设为可编辑类型
            jdbcTemplate.query(
                    connection -> connection.prepareStatement(sql,
                            ResultSet.TYPE_SCROLL_SENSITIVE,
                            ResultSet.CONCUR_UPDATABLE),
                    rs -> {
                        rs.moveToInsertRow();
                        rs.updateLong("cellid", cellid);
                        for (int i = 0; i < Math.min(columnNames.length, columnValues.size()); i++) {
                            rs.updateObject(columnNames[i], columnValues.get(i));
                        }
                        rs.insertRow();
                        return null;
                    }
            );
        }
    }

    @LoggingPoint("addRow")
    public void addRow(int cellid,
                       String[] columnNames,
                       List<Object> columnValues) {
        addRow((long) cellid, columnNames, columnValues);
    }


}
