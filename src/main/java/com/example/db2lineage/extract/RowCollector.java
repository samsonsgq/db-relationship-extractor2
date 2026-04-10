package com.example.db2lineage.extract;

import com.example.db2lineage.model.RelationshipRow;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public final class RowCollector {

    private final List<RelationshipRow> rows = new ArrayList<>();

    public void add(RelationshipRow row) {
        rows.add(row);
    }

    public List<RelationshipRow> rows() {
        return Collections.unmodifiableList(rows);
    }
}
