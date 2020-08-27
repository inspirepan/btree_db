package cn.edu.tsinghua.au.bioinfo.btree;

import btree4j.BTree;
import btree4j.BTreeException;
import btree4j.Value;

public class SingleBtreeInserter implements Runnable {
    private Value value;
    private long ID;
    private BTree bTree;

    public SingleBtreeInserter(BTree bTree, Value value, long ID) {
        this.bTree = bTree;
        this.ID = ID;
        this.value = value;
    }

    @Override
    public void run() {
        try {
            bTree.addValue(value, ID);
        } catch (BTreeException e) {
            e.printStackTrace();
        }
    }

}
