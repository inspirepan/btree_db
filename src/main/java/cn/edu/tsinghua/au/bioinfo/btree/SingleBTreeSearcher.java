package cn.edu.tsinghua.au.bioinfo.btree;

import btree4j.BTree;
import btree4j.BTreeCallback;
import btree4j.BTreeException;
import btree4j.Value;
import btree4j.indexer.BasicIndexQuery;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.Callable;

public class SingleBTreeSearcher implements Callable {
    private BTree bTree;
    private BasicIndexQuery condition;

    public SingleBTreeSearcher(BTree bTree, BasicIndexQuery condition) {
        this.bTree = bTree;
        this.condition = condition;
    }

    // 返回的是该bTree中满足条件condition的所有ID组成的set
    @Override
    public Set<Long> call() {
        SingleBTreeCallback callback = new SingleBTreeCallback();
        try {
            bTree.search(condition, callback);
        } catch (BTreeException e) {
            e.printStackTrace();
        }
        return callback.idSet;
    }
}

