package cn.edu.tsinghua.au.bioinfo.btree;

import btree4j.BTreeException;
import btree4j.BTreeIndex;
import btree4j.indexer.BasicIndexQuery;

import java.util.Set;
import java.util.concurrent.Callable;

/**
 * @author panjx
 */
public class SingleBtreeSearcher implements Callable<Set<Long>> {
    private final BTreeIndex bTree;
    private final BasicIndexQuery condition;

    public SingleBtreeSearcher(BTreeIndex bTree, BasicIndexQuery condition) {
        this.bTree = bTree;
        this.condition = condition;
    }

    /**
     * 返回的是该bTree中满足条件condition的所有ID组成的set
     */
    @Override
    public Set<Long> call() {
        SingleBtreeCallback callback = new SingleBtreeCallback();
        try {
            bTree.search(condition, callback);
        } catch (BTreeException e) {
            e.printStackTrace();
        }
        return callback.idSet;
    }
}

