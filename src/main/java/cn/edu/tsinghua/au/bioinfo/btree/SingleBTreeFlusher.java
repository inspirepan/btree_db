package cn.edu.tsinghua.au.bioinfo.btree;

import btree4j.BTree;
import btree4j.BTreeException;

public class SingleBTreeFlusher implements Runnable {
    private BTree bTree;

    public SingleBTreeFlusher(BTree bTree) {
        this.bTree = bTree;

    }

    @Override
    public void run() {
        try {
            bTree.flush();
        } catch (BTreeException e) {
            e.printStackTrace();
        }
    }

}
