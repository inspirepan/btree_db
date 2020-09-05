package cn.edu.tsinghua.au.bioinfo.btree;
import btree4j.BTreeException;
import btree4j.indexer.BasicIndexQuery;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutionException;

public interface IBTreeSearcher{

    void close() throws BTreeException, InterruptedException;

    void addColumn(String column) throws BTreeException;

    List<String> getColumnInfo();

    void insert(long ID, String[] columns, double[] values, int len)
            throws BTreeException, InterruptedException;

    Set<Long> rangeSearch(String[] columns, BasicIndexQuery[] conditions, int len)
            throws ExecutionException, InterruptedException, BTreeException;

    Set<Long> rangeSearch(String[] columns, BasicIndexQuery[] conditions, int len, Set<Long> candidates)
            throws ExecutionException, InterruptedException, BTreeException;

    void delete(long ID) throws BTreeException;

    void update(long ID, String columnName, double newV) throws BTreeException;
}
