package cn.edu.tsinghua.au.bioinfo.btree;

import btree4j.BTreeException;
import btree4j.indexer.BasicIndexQuery;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutionException;

@Component
public interface IBtreeSearcher {

    /**
     * 关闭所有BTree
     */
    void close() throws BTreeException, InterruptedException;

    /**
     * 添加一个column
     *
     * @param column 为dirFile路径下，需要添加的Btree文件名
     */
    void addColumn(String column) throws BTreeException;

    /**
     * 返回所有Column的名称列表
     */
    List<String> getColumnInfo();

    /**
     * 添加一行数据
     *
     * @param id      即对应的行
     * @param columns 数据对应的几列，即单独的BTree
     * @param values  数据，与columns一一对应
     * @param len     数据长度
     */
    void insert(long id, String[] columns, double[] values, int len)
            throws BTreeException, InterruptedException;

    /**
     * 搜索全部ID
     *
     * @param columns    指定若干个BTree
     * @param conditions 与上述BTree一一对应的条件
     * @param len        指定的BTree的数量
     * @return 返回满足所有条件的ID
     */
    Set<Long> rangeSearch(String[] columns, BasicIndexQuery[] conditions, int len)
            throws ExecutionException, InterruptedException, BTreeException;

    /**
     * 搜索指定ID
     *
     * @param columns    指定若干个BTree
     * @param conditions 与上述BTree一一对应的条件
     * @param len        指定的BTree的数量
     * @param candidates 指定的ID
     * @return 返回满足所有条件的ID
     */
    Set<Long> rangeSearch(String[] columns, BasicIndexQuery[] conditions, int len, Set<Long> candidates)
            throws ExecutionException, InterruptedException, BTreeException;

    /**
     * 将缓存写入文件中
     * 同时清空缓存
     */
    void flush() throws InterruptedException;

    /**
     * 按ID删除行
     *
     * @param id 指定的ID
     */
    void delete(long id) throws BTreeException;

    /**
     * 更新指定行列的数据
     *
     * @param id         指定ID（行）
     * @param columnName 指定BTree（列）
     * @param newV       新的值
     */
    void update(long id, String columnName, double newV) throws BTreeException;
}
