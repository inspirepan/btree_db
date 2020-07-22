package cn.edu.tsinghua.au.bioinfo.btree;

import btree4j.BTree;
import btree4j.BTreeException;
import btree4j.Value;

public class SingleBTreeInserter implements Runnable {
    private Value value;
    private long id;
    private BTree bTree;

    public SingleBTreeInserter(BTree bTree, Value value, long id) {
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
