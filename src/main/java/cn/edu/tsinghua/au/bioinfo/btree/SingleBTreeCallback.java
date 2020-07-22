package cn.edu.tsinghua.au.bioinfo.btree;

import btree4j.BTreeCallback;
import btree4j.Value;

import java.util.HashSet;
import java.util.Set;

public class SingleBTreeCallback implements BTreeCallback {

    Set<Long> idSet;

    public SingleBTreeCallback() {
        idSet = new HashSet<Long>();
    }

    @Override
    public boolean indexInfo(Value value, long l) {
        idSet.add(l);
        return false;
    }

    @Override
    public boolean indexInfo(Value value, byte[] bytes) {
        return false;
    }
}