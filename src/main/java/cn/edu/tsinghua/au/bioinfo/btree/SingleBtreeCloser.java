package cn.edu.tsinghua.au.bioinfo.btree;

import btree4j.BTree;
import btree4j.BTreeException;

public class SingleBtreeCloser implements Runnable {
    private BTree bTree;

    public SingleBtreeCloser(BTree bTree) {
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
