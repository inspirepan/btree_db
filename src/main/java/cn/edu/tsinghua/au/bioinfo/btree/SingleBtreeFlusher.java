package cn.edu.tsinghua.au.bioinfo.btree;

import btree4j.BTreeException;
import btree4j.BTreeIndex;

/**
 * @author panjx
 */
public class SingleBtreeFlusher implements Runnable {
    private final BTreeIndex bTree;

    public SingleBtreeFlusher(BTreeIndex bTree) {
        this.bTree = bTree;

    }

    @Override
    public void run() {
        try {
            // clear设置为true，清空缓存
            bTree.flush(true,true);
        } catch (BTreeException e) {
            e.printStackTrace();
        }
    }

}
