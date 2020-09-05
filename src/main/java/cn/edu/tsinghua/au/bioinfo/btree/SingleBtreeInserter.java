package cn.edu.tsinghua.au.bioinfo.btree;

import btree4j.BTree;
import btree4j.BTreeException;
import btree4j.Value;

/**
 * @author panjx
 */
public class SingleBtreeInserter implements Runnable {
    private final Value value;
    private final long id;
    private final BTree bTree;

    public SingleBtreeInserter(BTree bTree, Value value, long id) {
        this.bTree = bTree;
        this.id = id;
        this.value = value;
    }

    @Override
    public void run() {
        try {
            bTree.addValue(value, id);
        } catch (BTreeException e) {
            e.printStackTrace();
        }
    }

}
