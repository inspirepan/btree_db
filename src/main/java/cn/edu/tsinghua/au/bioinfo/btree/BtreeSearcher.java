package cn.edu.tsinghua.au.bioinfo.btree;

import btree4j.BTreeException;
import btree4j.BTreeIndex;
import btree4j.Value;
import btree4j.indexer.BasicIndexQuery;
import org.slf4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.io.File;
import java.util.*;
import java.util.concurrent.*;

/**
 * @author panjx
 */
@Component
public class BtreeSearcher implements IBtreeSearcher {

    private final File dirFile;
    private final Map<String, BTreeIndex> map;
    private final Logger log;

    public BtreeSearcher(@Autowired BtreeInitializer bTreeInitializer, @Autowired Logger log) {
        this.map = bTreeInitializer.getBtreeMap();
        this.dirFile = bTreeInitializer.getDirFile();
        this.log = log;
    }

    @Override
    public void close() throws InterruptedException {
        ExecutorService es = new ThreadPoolExecutor(5, 10,
                1L, TimeUnit.SECONDS,
                new LinkedBlockingQueue<>(8),
                Executors.defaultThreadFactory(),
                new ThreadPoolExecutor.AbortPolicy());
        for (BTreeIndex bTree : map.values()) {
            SingleBtreeCloser singleBtreeCloser = new SingleBtreeCloser(bTree);
            es.execute(singleBtreeCloser);
        }
        es.shutdown();
        es.awaitTermination(1, TimeUnit.HOURS);
    }

    @Override
    @BtreeLoggingPoint("addColumn")
    public void addColumn(String column) throws BTreeException {
        if (map.containsKey(column)) {
            log.error("trying to add column {} which already exits", column);
            return;
        }
        File file = new File(dirFile, column);
        BTreeIndex bTree = new BTreeIndex(file);
        bTree.init(false);
        map.put(column, bTree);
    }

    @Override
    public List<String> getColumnInfo() {
        List<String> columnInfo = new ArrayList<>(map.keySet());
        log.info("column names " + columnInfo.toString());
        return columnInfo;
    }

    @Override
    @BtreeLoggingPoint("insertId")
    public void insert(long id, String[] columns, double[] values, int len) throws InterruptedException {
        // TODO 检查id是否存在否则update
        // TODO 检查len有效性
        ExecutorService executorService = new ThreadPoolExecutor(5, 10,
                1L, TimeUnit.SECONDS,
                new LinkedBlockingQueue<>(5),
                Executors.defaultThreadFactory(),
                new ThreadPoolExecutor.AbortPolicy());
        for (int i = 0; i < len; i++) {
            if (!map.containsKey(columns[i])) {
                // TODO 抛异常
                continue;
            }
            BTreeIndex bTree = map.get(columns[i]);
            SingleBtreeInserter singleBtreeInserter = new SingleBtreeInserter(bTree, id, values[i]);
            executorService.execute(singleBtreeInserter);
        }
        executorService.shutdown();
        executorService.awaitTermination(1, TimeUnit.HOURS);
    }

    @Override
    public void flush() throws InterruptedException {
        ExecutorService executorService = new ThreadPoolExecutor(2, 5,
                1L, TimeUnit.SECONDS,
                new LinkedBlockingQueue<>(3),
                Executors.defaultThreadFactory(),
                new ThreadPoolExecutor.AbortPolicy());
        for (BTreeIndex bTree : map.values()) {
            SingleBtreeFlusher singleBtreeFlusher = new SingleBtreeFlusher(bTree);
            executorService.execute(singleBtreeFlusher);
        }
        executorService.shutdown();
        executorService.awaitTermination(1, TimeUnit.HOURS);
    }

    @Override
    @BtreeLoggingPoint("rangeSearch")
    public Set<Long> rangeSearch(String[] columns, BasicIndexQuery[] conditions, int len)
            throws ExecutionException, InterruptedException, BTreeException {
        Set<Long> resultSet = new HashSet<>();
        return rangeSearch(columns, conditions, len, resultSet);
    }

    @Override
    @SuppressWarnings("unchecked")
    @BtreeLoggingPoint("rangeSearch")
    public Set<Long> rangeSearch(String[] columns, BasicIndexQuery[] conditions, int len, Set<Long> candidates)
            throws ExecutionException, InterruptedException, BTreeException {
        // TODO 检查len有效性
        Future<Set<Long>>[] futures = new Future[len];
        ExecutorService executorService = new ThreadPoolExecutor(2, 5,
                1L, TimeUnit.SECONDS,
                new LinkedBlockingQueue<>(3),
                Executors.defaultThreadFactory(),
                new ThreadPoolExecutor.AbortPolicy());
        for (int i = 0; i < len; i++) {
            BTreeIndex bTree = map.get(columns[i]);
            if (bTree == null) {
                // TODO 列名不存在，报错
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

    @Override
    @BtreeLoggingPoint("deleteId")
    public void delete(long id) throws BTreeException {
        for (BTreeIndex bTree : map.values()) {
            bTree.remove(new Value(id));
        }
    }

    @Override
    @BtreeLoggingPoint("updateId")
    public void update(long id, String columnName, double newV) throws BTreeException {
        // TODO 列名不存在报错
        if (!map.containsKey(columnName)) {
            log.error("column {} not found.", columnName);
            return;
        }
        BTreeIndex bTree = map.get(columnName);
        bTree.remove(new Value(id));
        bTree.addValue(new Value((long) newV), id);
    }

    public Set<Long> getAllIds() throws BTreeException {
        Set<Long> idSet = new HashSet<>();
        for (BTreeIndex bTree : map.values()) {
            SingleBtreeCallback sbc = new SingleBtreeCallback();
            bTree.search(new BasicIndexQuery.IndexConditionANY(), sbc);
            idSet.addAll(sbc.idSet);
        }
        return idSet;
    }
}
