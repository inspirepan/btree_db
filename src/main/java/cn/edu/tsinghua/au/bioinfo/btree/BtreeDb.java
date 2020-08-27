package cn.edu.tsinghua.au.bioinfo.btree;

import btree4j.BTree;
import btree4j.BTreeException;

import java.io.File;
import java.util.HashMap;
import java.util.Map;

public class BtreeDb {

    private final File dirFile;
    private final Map<String, BTree> bTreeMap = new HashMap<>();


    BtreeDb(String dirPath1) {
        dirFile = new File(dirPath1);
        if (dirFile.exists()) {
            try {
                recover(dirFile);
            } catch (BTreeException e) {
                e.printStackTrace();
            }
        } else {
            dirFile.mkdirs();
        }
    }

    public Map<String, BTree> getBtreeMap() {
        return this.bTreeMap;
    }

    public File getDirFile(){
        return this.dirFile;
    }

    /**
     * 将File对象的路径下所有的文件添加到bTreeMap中 key为单个文件的文件名，相当于column value为单个文件构造的BTree
     *
     * @param dir File对象
     */
    private void recover(File dir) throws BTreeException {
        File[] files = dir.listFiles();
        for (File file : files) {
            String column = file.getName();
            System.out.println("recovering " + column);
            BTree bTree = new BTree(file);
            bTree.init(false);
            bTreeMap.put(column, bTree);
        }
    }
}
