package cn.edu.tsinghua.au.bioinfo.btree;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.Set;
import java.util.HashSet;
import java.util.List;
import java.util.StringJoiner;

class mysqlBasedDbException extends Exception {
    mysqlBasedDbException(String s) {
        super(s);
    }
}

public class mysqlBasedDb {

    static final String DRIVER = "com.mysql.cj.jdbc.Driver";
    static final String URL = "jdbc:mysql://localhost:3306/?allowPublicKeyRetrieval=true&serverTimezone=UTC";
    static final String USER = "root";
    static final String PASS = "root";
    Connection conn = null;

    public mysqlBasedDb() {
    }

    // 连接数据库, 获取Statement类实例
    public void connect() {
        try {
            Class.forName(DRIVER);
            System.out.println("Connecting...");
            conn = DriverManager.getConnection(URL, USER, PASS);
        } catch (Exception se) {
            se.printStackTrace();
        }
    }

    // 关闭数据库连接
    public void close() {
        try {
            if (conn != null) conn.close();
            System.out.println("Exited");
        } catch (SQLException se) {
            se.printStackTrace();
        }
    }

    // 添加行
    public void addRow(Long cellid,
                       String[] columnNames,
                       List<Object> columnValues)
            throws mysqlBasedDbException {
        if (columnNames.length != columnValues.size())
            throw new mysqlBasedDbException("The number of names and values don't match");
        try {
            Statement stmt = conn.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_UPDATABLE);
            ResultSet rs = stmt.executeQuery("SELECT * FROM hcaddb.new_table");
            rs.moveToInsertRow();
            rs.updateLong("cellid", cellid);
            for (int i = 0; i < columnNames.length; i++) {
                rs.updateObject(columnNames[i], columnValues.get(i));
            }
            rs.insertRow();
            System.out.println("Add finished");
            rs.close();
        } catch (SQLException se) {
            se.printStackTrace();
        }
    }

    // 以String相等搜索, 范围是全部ID
    public Set<Long> queryByStringEqual(String[] columnNames,
                                        String[] columnValues)
            throws mysqlBasedDbException {
        if (columnNames.length != columnValues.length)
            throw new mysqlBasedDbException("The number of names and values don't match");
        Set<Long> result = new HashSet<>();
        try {
            Statement stmt = conn.createStatement();
            ResultSet rs = stmt.executeQuery("SELECT * FROM hcaddb.new_table");
            while (rs.next()) {
                boolean stringEqual = true;
                for (int i = 0; i < columnNames.length; i++) {
                    if (!rs.getString(columnNames[i]).equals(columnValues[i])) {
                        stringEqual = false;
                        break;
                    }
                }
                if (stringEqual) result.add(rs.getLong("cellid"));

            }
            System.out.println("Query finished");
            rs.close();
            stmt.close();
        } catch (SQLException se2) {
            se2.printStackTrace();
        }
        return result;
    }

    // 重载, 在指定的ID范围搜索
    public Set<Long> queryByStringEqual(String[] columnNames,
                                        String[] columnValues,
                                        Set<Long> candidate)
            throws mysqlBasedDbException {
        if (columnNames.length != columnValues.length)
            throw new mysqlBasedDbException("The number of names and values don't match");
        Set<Long> result = new HashSet<>();
        try {

            String sql = "SELECT * FROM hcaddb.new_table WHERE FIND_IN_SET(`cellid`, ?)";
            PreparedStatement stmt = conn.prepareStatement(sql);
            StringJoiner sj = new StringJoiner(",");
            for (Long cd : candidate) sj.add(cd.toString());
            stmt.setString(1, sj.toString());
            ResultSet rs = stmt.executeQuery();
            while (rs.next()) {
                boolean stringEqual = true;
                for (int i = 0; i < columnNames.length; i++) {
                    if (!rs.getString(columnNames[i]).equals(columnValues[i])) {
                        stringEqual = false;
                        break;
                    }
                }
                if (stringEqual) result.add(rs.getLong("cellid"));
            }
            System.out.println("Query finished");
            stmt.close();
            rs.close();
        } catch (SQLException se3) {
            se3.printStackTrace();
        }
        return result;
    }
}
