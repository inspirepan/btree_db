package cn.edu.tsinghua.au.bioinfo.btree;

import btree4j.BTreeException;
import btree4j.BTreeIndex;
import btree4j.utils.lang.Primitives;

/**
 * @author panjx
 */
public class SingleBtreeInserter implements Runnable {
    private final double value;
    private final long key;
    private final BTreeIndex bTree;

    public SingleBtreeInserter(BTreeIndex bTree, long id, double value) {
        this.bTree = bTree;
        this.key = id;
        this.value = value;
    }

    @Override
    public void run() {
        try {
            bTree.addValue(key, Primitives.toBytes(value));
        } catch (BTreeException e) {
            e.printStackTrace();
        }
    }
}
