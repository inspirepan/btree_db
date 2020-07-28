package cn.edu.tsinghua.au.bioinfo.btree;

import btree4j.BTree;
import btree4j.BTreeException;

public class App {
    public static void main(String[] args) {

        try {
            BTreeSearcher btree = new BTreeSearcher("E://Onedrive//code//btree_data");
            System.out.println(btree.getColumnInfo());
        } catch (BTreeException e) {
            e.printStackTrace();
        }


    }
}
