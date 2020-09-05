package cn.edu.tsinghua.au.bioinfo.btree;

import btree4j.BTreeException;
import btree4j.BTreeIndex;

/**
 * @author panjx
 */
public class SingleBtreeCloser implements Runnable {
    private final BTreeIndex bTree;

    public SingleBtreeCloser(BTreeIndex bTree) {
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
