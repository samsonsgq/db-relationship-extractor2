package com.example.db2lineage.model;

public enum RelationshipType {
    CREATE_VIEW,
    SELECT_TABLE,
    SELECT_VIEW,
    INSERT_TABLE,
    INSERT_TARGET_COL,
    INSERT_SELECT_MAP,
    UPDATE_TABLE,
    UPDATE_TARGET_COL,
    UPDATE_SET,
    UPDATE_SET_MAP,
    UNKNOWN
}
