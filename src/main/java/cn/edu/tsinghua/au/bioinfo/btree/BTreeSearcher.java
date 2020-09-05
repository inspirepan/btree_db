package cn.edu.tsinghua.au.bioinfo.btree;

import btree4j.BTree;
import btree4j.BTreeException;
import btree4j.Value;
import btree4j.indexer.BasicIndexQuery;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.io.File;
import java.util.*;
import java.util.concurrent.*;


/**
 * @author panjx
 */
@Component
public class BtreeSearcher implements IBTreeSearcher {

    private final File dirFile;
    private final Map<String, BTree> map;


    public BtreeSearcher(@Autowired BtreeDb bTreeDb) {
        this.map = bTreeDb.getBtreeMap();
        this.dirFile = bTreeDb.getDirFile();
    }

    /**
     * 关闭所有BTree
     */
    @Override
    public void close() throws InterruptedException {
        ExecutorService es = new ThreadPoolExecutor(5, 10,
                1L, TimeUnit.SECONDS,
                new LinkedBlockingQueue<>(8),
                Executors.defaultThreadFactory(),
                new ThreadPoolExecutor.AbortPolicy());
        for (BTree bTree : map.values()) {
            SingleBtreeCloser singleBtreeCloser = new SingleBtreeCloser(bTree);
            es.execute(singleBtreeCloser);
        }
        es.shutdown();
        es.awaitTermination(1, TimeUnit.HOURS);
    }

    /**
     * 添加一个column
     *
     * @param column 为dirFile路径下，需要添加的Btree文件名
     */
    @Override
    public void addColumn(String column) throws BTreeException {
        File file = new File(dirFile, column);
        BTree bTree = new BTree(file);
        bTree.init(false);
        map.put(column, bTree);
    }

    /**
     * 返回所有Column的名称列表
     */
    @Override
    public List<String> getColumnInfo() {
        return new ArrayList<>(map.keySet());
    }

    /**
     * 添加一行数据
     *
     * @param id      即对应的行
     * @param columns 数据对应的几列，即单独的BTree
     * @param values  数据，与columns一一对应
     * @param len     数据长度
     */
    @Override
    public void insert(long id, String[] columns, double[] values, int len) throws InterruptedException {
        ExecutorService executorService = new ThreadPoolExecutor(5, 10,
                1L, TimeUnit.SECONDS,
                new LinkedBlockingQueue<>(5),
                Executors.defaultThreadFactory(),
                new ThreadPoolExecutor.AbortPolicy());
        for (int i = 0; i < len; i++) {
            if (!map.containsKey(columns[i])) {
                continue;
            }
            BTree bTree = map.get(columns[i]);
            SingleBtreeInserter singleBtreeInserter = new SingleBtreeInserter(bTree, new Value((long) values[i]), id);
            executorService.execute(singleBtreeInserter);
        }
        executorService.shutdown();
        executorService.awaitTermination(1, TimeUnit.HOURS);
    }

    /**
     * 刷新缓存区
     */
    public void flush() throws InterruptedException {
        ExecutorService executorService = new ThreadPoolExecutor(2, 5,
                1L, TimeUnit.SECONDS,
                new LinkedBlockingQueue<>(3),
                Executors.defaultThreadFactory(),
                new ThreadPoolExecutor.AbortPolicy());
        for (BTree bTree : map.values()) {
            SingleBtreeFlusher singleBtreeFlusher = new SingleBtreeFlusher(bTree);
            executorService.execute(singleBtreeFlusher);
        }
        executorService.shutdown();
        executorService.awaitTermination(1, TimeUnit.HOURS);
    }

    /**
     * 搜索全部ID
     *
     * @param columns    指定若干个BTree
     * @param conditions 与上述BTree一一对应的条件
     * @param len        指定的BTree的数量
     * @return 返回满足所有条件的ID
     */
    @Override
    public Set<Long> rangeSearch(String[] columns, BasicIndexQuery[] conditions, int len)
            throws ExecutionException, InterruptedException, BTreeException {
        Set<Long> resultSet = new HashSet<>();
        return rangeSearch(columns, conditions, len, resultSet);
    }

    /**
     * 搜索指定ID
     *
     * @param columns    指定若干个BTree
     * @param conditions 与上述BTree一一对应的条件
     * @param len        指定的BTree的数量
     * @param candidates 指定的ID
     * @return 返回满足所有条件的ID
     */
    @Override
    @SuppressWarnings("unchecked")
    public Set<Long> rangeSearch(String[] columns, BasicIndexQuery[] conditions, int len, Set<Long> candidates)
            throws ExecutionException, InterruptedException, BTreeException {
        Future<Set<Long>>[] futures = new Future[len];
        ExecutorService executorService = new ThreadPoolExecutor(2, 5,
                1L, TimeUnit.SECONDS,
                new LinkedBlockingQueue<>(3),
                Executors.defaultThreadFactory(),
                new ThreadPoolExecutor.AbortPolicy());
        for (int i = 0; i < len; i++) {
            BTree bTree = map.get(columns[i]);
            if (bTree == null) {
                throw new BTreeException();
            }
            SingleBtreeSearcher singleSearcher = new SingleBtreeSearcher(bTree, conditions[i]);
            futures[i] = executorService.submit(singleSearcher);
        }
        for (int i = 0; i < len; i++) {
            if (candidates.isEmpty()) {
                // 对于无候选搜索, 先初始化, 添加满足第0个条件的所有ID
                candidates.addAll(futures[i].get());
            } else {
                // 每一次保留满足条件i的所有ID
                candidates.retainAll(futures[i].get());
            }
        }
        executorService.shutdown();
        executorService.awaitTermination(1, TimeUnit.HOURS);
        return candidates;
    }

    /**
     * 按ID删除行
     *
     * @param id 指定的ID
     */
    @Override
    public void delete(long id) throws BTreeException {
        for (BTree bTree : map.values()) {
            bTree.removeValue(new Value(id));
        }
    }

    /**
     * 更新指定行列的数据
     *
     * @param id         指定ID（行）
     * @param columnName 指定BTree（列）
     * @param newV       新的值
     */

    @Override
    public void update(long id, String columnName, double newV) throws BTreeException {
        BTree bTree = map.get(columnName);
        if (bTree == null) {
            throw new BTreeException("No such column.");
        }

        bTree.removeValue(new Value(id));
        bTree.addValue(new Value((long) newV), id);
    }

    public Set<Long> getAllIds() throws BTreeException {
        Set<Long> idSet = new HashSet<>();
        for (BTree bTree : map.values()) {
//             idSet.addAll(bTree.searchSet(new IndexConditionANY())); 添加这个bTree中的所有ID
        }
        return idSet;
    }
}
