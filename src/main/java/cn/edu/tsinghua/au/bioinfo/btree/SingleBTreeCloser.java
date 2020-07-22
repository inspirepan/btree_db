package cn.edu.tsinghua.au.bioinfo.btree;

import btree4j.BTree;
import btree4j.BTreeException;

public class SingleBTreeCloser implements Runnable {
    private BTree bTree;

    public SingleBTreeCloser(BTree bTree) {
        this.bTree = bTree;
    }

    @Override
    public void run() {
        try {
            bTree.close();
        } catch (BTreeException e) {
            e.printStackTrace();
        }
    }
}
