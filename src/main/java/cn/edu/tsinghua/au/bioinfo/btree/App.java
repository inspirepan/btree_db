package cn.edu.tsinghua.au.bioinfo.btree;

import btree4j.BTree;
import btree4j.BTreeException;

public class App {
    public static void main(String[] args) {

        try {
            BTreeSearcher btree = new BTreeSearcher("E://Onedrive//code//btree_data");
        } catch (BTreeException e) {
            e.printStackTrace();
        }


    }
}
