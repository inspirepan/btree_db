package cn.edu.tsinghua.au.bioinfo.btree;

import btree4j.BTreeCallback;
import btree4j.Value;
import btree4j.utils.lang.Primitives;

import java.util.HashSet;
import java.util.Set;

public class SingleBtreeCallback implements BTreeCallback {

    public Set<Long> idSet;

    public SingleBtreeCallback() {
        this.idSet = new HashSet<Long>();
    }

    @Override
    public boolean indexInfo(Value value, long pointer) {
        idSet.add(pointer);
        return false;
    }

    @Override
    public boolean indexInfo(Value value, byte[] bytes) {
        idSet.add(Primitives.getLong(bytes, 0));
        return false;
    }
}