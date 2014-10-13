package com.expedia.edw.hbase.constants;

/**
 * HBase table columns.
 */
public class HBColumn {

    private final byte[] colFamily;
    private final byte[] colQualifier;

    public HBColumn(String colFamily, String colQualifier) {
        this.colFamily = colFamily.getBytes();
        this.colQualifier = colQualifier.getBytes();
    }

    public byte[] getColFamily() {
        return colFamily;
    }

    public byte[] getColQualifier() {
        return colQualifier;
    }
}