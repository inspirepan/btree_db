package cn.edu.tsinghua.au.bioinfo.btree;

import btree4j.BTree;
import btree4j.BTreeException;

public class SingleBtreeFlusher implements Runnable {
    private BTree bTree;

    public SingleBtreeFlusher(BTree bTree) {
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
