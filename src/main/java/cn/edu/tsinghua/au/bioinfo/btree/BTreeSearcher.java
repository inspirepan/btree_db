package cn.edu.tsinghua.au.bioinfo.btree;

import btree4j.BTree;
import btree4j.BTreeException;
import btree4j.Value;
import btree4j.indexer.BasicIndexQuery;
import btree4j.indexer.BasicIndexQuery.IndexConditionANY;
import btree4j.indexer.BasicIndexQuery.IndexConditionBW;
import btree4j.indexer.BasicIndexQuery.IndexConditionGT;
import btree4j.indexer.BasicIndexQuery.IndexConditionLT;

import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

public class BTreeSearcher implements IBTreeSearcher {

    private final int THREAD_NUM = 64;

    private Map<String, BTree> bTreeMap = new HashMap<String, BTree>();
    private String dirPath;
    private File dirFile;

    /* 构造函数, 参数为路径 */
    public BTreeSearcher(String dirPath) throws BTreeException {
        this.dirPath = dirPath;
        this.dirFile = new File(dirPath);
        if (dirFile.exists()) {
            recover(dirFile);
        } else {
            dirFile.mkdirs();
        }
    }

    /* 构造函数中调用, 对File对象的路径下所有的文件, 添加到bTreeMap中,
    column为文件名, 以及以file构造的BTree */
    private void recover(File dir) throws BTreeException {
        File[] files = dir.listFiles();
        for (File file : files) {
            String column = file.getName();
            System.out.println("recovering "+column);
            BTree bTree = new BTree(file);
            bTree.init(false);
            bTreeMap.put(column, bTree);
        }
    }

    /* 关闭 */
    public void close() throws InterruptedException {
        ExecutorService executorService = Executors.newFixedThreadPool(THREAD_NUM);
        for (BTree bTree : bTreeMap.values()) {
            SingleBTreeCloser singleBTreeCloser = new SingleBTreeCloser(bTree);
            executorService.execute(singleBTreeCloser);
        }
        executorService.shutdown();
        executorService.awaitTermination(1, TimeUnit.HOURS);
    }

    /* 添加一个Column, 需要先有对应的文件 */
    public void addColumn(String column) throws BTreeException {
        File file = new File(dirFile, column);
        BTree bTree = new BTree(file);
        bTree.init(false);
        bTreeMap.put(column, bTree);
    }

    /* 返回所有Column的名称列表 */
    public List<String> getColumnInfo() {
        return new ArrayList<String>(bTreeMap.keySet());
    }

    /* 添加一行数据, 输入参数为ID, 需要添加的列名, 对应的值, 长度 */
    public void insert(long ID, String[] columns, double[] values, int len)
            throws InterruptedException {
        ExecutorService executorService = Executors.newFixedThreadPool(THREAD_NUM);
        for (int i = 0; i < len; i++) {
            if (!bTreeMap.containsKey(columns[i])) {
                continue;
            }
            BTree bTree = bTreeMap.get(columns[i]);
            SingleBTreeInserter singleBTreeInserter = new SingleBTreeInserter(bTree, new Value((long) values[i]), ID);
            executorService.execute(singleBTreeInserter);
        }
        executorService.shutdown();
        executorService.awaitTermination(1, TimeUnit.HOURS);
    }


    /* 刷新 */
    public void flush() throws InterruptedException {
        ExecutorService executorService = Executors.newFixedThreadPool(THREAD_NUM);
        for (BTree bTree : bTreeMap.values()) {
            SingleBTreeFlusher singleBTreeFlusher = new SingleBTreeFlusher(bTree);
            executorService.execute(singleBTreeFlusher);
        }
        executorService.shutdown();
        executorService.awaitTermination(1, TimeUnit.HOURS);
    }

    /* 搜索全部范围 */
    public Set<Long> rangeSearch(String[] columns, BasicIndexQuery[] conditions, int len)
            throws ExecutionException, InterruptedException, BTreeException {
        Set<Long> resultSet = new HashSet<Long>();
        return rangeSearch(columns, conditions, len, resultSet);
    }

    /* 重载, 搜索指定ID范围 */
    public Set<Long> rangeSearch(String[] columns, BasicIndexQuery[] conditions, int len,
                                 Set<Long> candidates) throws ExecutionException, InterruptedException, BTreeException {
        Future<Set<Long>>[] futures = new Future[len];
        ExecutorService executorService = Executors.newFixedThreadPool(THREAD_NUM);
        for (int i = 0; i < len; i++) {
            BTree bTree = bTreeMap.get(columns[i]);
            if (bTree == null) {
                throw new BTreeException();
            }

            SingleBTreeSearcher singleSearcher = new SingleBTreeSearcher(bTree, conditions[i]);
            futures[i] = executorService.submit(singleSearcher);
        }

        for (int i = 0; i < len; i++) {
            // candidates为空对应重载的搜索全部范围
            if (candidates.isEmpty()) {
                // 对于无候选搜索, 先初始化, 添加满足第0个条件的所有ID
                candidates.addAll(futures[i].get());
            } else {
                // 保留满足条件i的所有ID
                candidates.retainAll(futures[i].get());
            }
        }
        executorService.shutdown();
        executorService.awaitTermination(1, TimeUnit.HOURS);
        return candidates;
    }

    /* 按 ID 删除行 */
    public void delete(long ID) throws BTreeException {
        for (BTree bTree : bTreeMap.values()) {
            bTree.removeValue(new Value(ID));
        }
    }

    public void update(long ID, String columnName, double newV) throws BTreeException {
        BTree bTree = bTreeMap.get(columnName);
        if (bTree == null) {
            throw new BTreeException("No such column.");
        }

        bTree.removeValue(new Value(ID));
        bTree.addValue(new Value((long) newV), ID);
    }

    public Set<Long> getAllIDs() throws BTreeException {
        Set<Long> idSet = new HashSet<Long>();
        for (BTree bTree : bTreeMap.values()) {
//            idSet.addAll(bTree.searchSet(new IndexConditionANY())); 添加这个bTree中的所有ID
        }
        return idSet;
    }
}
