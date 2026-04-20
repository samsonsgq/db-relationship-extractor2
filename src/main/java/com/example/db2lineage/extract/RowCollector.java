package com.example.db2lineage.extract;

import com.example.db2lineage.model.RelationshipRow;
import com.example.db2lineage.model.RowDraft;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

public final class RowCollector {

    private final List<RowDraft> drafts = new ArrayList<>();

    public void addDraft(RowDraft draft) {
        drafts.add(Objects.requireNonNull(draft, "draft must not be null"));
    }

    public List<RowDraft> drafts() {
        return Collections.unmodifiableList(drafts);
    }

    public List<RelationshipRow> finalizeRows() {
        Map<SemanticKey, RowDraft> deduplicated = new LinkedHashMap<>();
        for (RowDraft draft : drafts) {
            deduplicated.putIfAbsent(SemanticKey.from(draft), draft);
        }

        List<RowDraft> filteredDrafts = suppressUnknownWhenResolvedExists(new ArrayList<>(deduplicated.values()));

        Map<GroupKey, List<RowDraft>> grouped = new LinkedHashMap<>();
        for (RowDraft draft : filteredDrafts) {
            grouped.computeIfAbsent(GroupKey.from(draft), ignored -> new ArrayList<>()).add(draft);
        }

        List<RelationshipRow> finalized = new ArrayList<>();
        for (List<RowDraft> groupRows : grouped.values()) {
            groupRows.sort(Comparator
                    .comparingInt(RowDraft::statementOrder)
                    .thenComparingInt(draft -> relationshipBucketRank(draft.relationship()))
                    .thenComparingInt(RowDraft::naturalOrderOnLine)
                    .thenComparingInt(draft -> relationshipFamilyRank(draft.relationship()))
                    .thenComparing(RowDraft::targetObjectType)
                    .thenComparing(RowDraft::targetObject)
                    .thenComparing(RowDraft::targetField)
                    .thenComparing(RowDraft::sourceField)
                    .thenComparing(RowDraft::lineContent)
            );

            for (int i = 0; i < groupRows.size(); i++) {
                RowDraft draft = groupRows.get(i);
                finalized.add(new RelationshipRow(
                        draft.sourceObjectType(),
                        draft.sourceObject(),
                        draft.sourceField(),
                        draft.targetObjectType(),
                        draft.targetObject(),
                        draft.targetField(),
                        draft.relationship(),
                        draft.lineNo(),
                        i,
                        draft.lineContent(),
                        draft.confidence()
                ));
            }
        }

        finalized.sort(RelationshipRow.STABLE_OUTPUT_COMPARATOR);
        return List.copyOf(finalized);
    }

    private static List<RowDraft> suppressUnknownWhenResolvedExists(List<RowDraft> rows) {
        Map<String, Boolean> hasResolved = new LinkedHashMap<>();
        for (RowDraft row : rows) {
            String key = row.sourceObjectType() + "|" + row.sourceObject() + "|" + row.relationship()
                    + "|" + row.sourceField();
            if (row.targetObjectType() != com.example.db2lineage.model.TargetObjectType.UNKNOWN) {
                hasResolved.put(key, true);
            } else {
                hasResolved.putIfAbsent(key, false);
            }
        }
        List<RowDraft> filtered = new ArrayList<>();
        for (RowDraft row : rows) {
            String key = row.sourceObjectType() + "|" + row.sourceObject() + "|" + row.relationship()
                    + "|" + row.sourceField();
            boolean resolvedExists = Boolean.TRUE.equals(hasResolved.get(key));
            if (resolvedExists && row.targetObjectType() == com.example.db2lineage.model.TargetObjectType.UNKNOWN) {
                continue;
            }
            filtered.add(row);
        }
        return filtered;
    }

    private static int relationshipBucketRank(com.example.db2lineage.model.RelationshipType relationship) {
        if (OBJECT_LEVEL_RELATIONSHIPS.contains(relationship)) {
            return 0;
        }
        if (USAGE_SIDE_RELATIONSHIPS.contains(relationship)) {
            return 1;
        }
        return 2;
    }

    private static int relationshipFamilyRank(com.example.db2lineage.model.RelationshipType relationship) {
        return switch (relationship) {
            case CREATE_VIEW -> 0;
            case CREATE_TABLE -> 1;
            case CREATE_PROCEDURE -> 2;
            case CREATE_FUNCTION -> 3;
            case SELECT_TABLE -> 4;
            case SELECT_VIEW -> 5;
            case INSERT_TABLE -> 6;
            case UPDATE_TABLE -> 7;
            case DELETE_TABLE -> 8;
            case DELETE_VIEW -> 9;
            case TRUNCATE_TABLE -> 10;
            case MERGE_INTO -> 11;
            case CALL_FUNCTION -> 12;
            case CALL_PROCEDURE -> 13;
            case CTE_DEFINE -> 14;
            case CTE_READ -> 15;
            case UNION_INPUT -> 16;
            case CURSOR_DEFINE -> 17;
            case CURSOR_READ -> 18;
            case DYNAMIC_SQL_EXEC -> 19;
            case EXCEPTION_HANDLER_MAP -> 20;
            case UNKNOWN -> 21;

            case SELECT_FIELD -> 30;
            case SELECT_EXPR -> 31;
            case UPDATE_SET -> 32;
            case WHERE -> 33;
            case JOIN_ON -> 34;
            case MERGE_MATCH -> 35;
            case GROUP_BY -> 36;
            case ORDER_BY -> 37;
            case HAVING -> 38;
            case CONTROL_FLOW_CONDITION -> 39;
            case RETURN_VALUE -> 40;

            case INSERT_TARGET_COL -> 50;
            case UPDATE_TARGET_COL -> 51;
            case MERGE_TARGET_COL -> 52;
            case CREATE_VIEW_MAP -> 53;
            case INSERT_SELECT_MAP -> 54;
            case UPDATE_SET_MAP -> 55;
            case MERGE_SET_MAP -> 56;
            case MERGE_INSERT_MAP -> 57;
            case VARIABLE_SET_MAP -> 58;
            case CURSOR_FETCH_MAP -> 59;
            case FUNCTION_PARAM_MAP -> 60;
            case CALL_PARAM_MAP -> 61;
            case TABLE_FUNCTION_RETURN_MAP -> 62;
            case SPECIAL_REGISTER_MAP -> 63;
            case DIAGNOSTICS_FETCH_MAP -> 64;
            case FUNCTION_EXPR_MAP -> 65;
        };
    }

    private static final Set<com.example.db2lineage.model.RelationshipType> OBJECT_LEVEL_RELATIONSHIPS = Set.of(
            com.example.db2lineage.model.RelationshipType.CREATE_VIEW,
            com.example.db2lineage.model.RelationshipType.CREATE_TABLE,
            com.example.db2lineage.model.RelationshipType.CREATE_PROCEDURE,
            com.example.db2lineage.model.RelationshipType.CREATE_FUNCTION,
            com.example.db2lineage.model.RelationshipType.SELECT_TABLE,
            com.example.db2lineage.model.RelationshipType.SELECT_VIEW,
            com.example.db2lineage.model.RelationshipType.INSERT_TABLE,
            com.example.db2lineage.model.RelationshipType.UPDATE_TABLE,
            com.example.db2lineage.model.RelationshipType.MERGE_INTO,
            com.example.db2lineage.model.RelationshipType.DELETE_TABLE,
            com.example.db2lineage.model.RelationshipType.DELETE_VIEW,
            com.example.db2lineage.model.RelationshipType.TRUNCATE_TABLE,
            com.example.db2lineage.model.RelationshipType.CALL_FUNCTION,
            com.example.db2lineage.model.RelationshipType.CALL_PROCEDURE,
            com.example.db2lineage.model.RelationshipType.CTE_DEFINE,
            com.example.db2lineage.model.RelationshipType.CTE_READ,
            com.example.db2lineage.model.RelationshipType.UNION_INPUT,
            com.example.db2lineage.model.RelationshipType.CURSOR_DEFINE,
            com.example.db2lineage.model.RelationshipType.CURSOR_READ,
            com.example.db2lineage.model.RelationshipType.DYNAMIC_SQL_EXEC,
            com.example.db2lineage.model.RelationshipType.EXCEPTION_HANDLER_MAP,
            com.example.db2lineage.model.RelationshipType.UNKNOWN
    );

    private static final Set<com.example.db2lineage.model.RelationshipType> USAGE_SIDE_RELATIONSHIPS = Set.of(
            com.example.db2lineage.model.RelationshipType.SELECT_FIELD,
            com.example.db2lineage.model.RelationshipType.SELECT_EXPR,
            com.example.db2lineage.model.RelationshipType.UPDATE_SET,
            com.example.db2lineage.model.RelationshipType.WHERE,
            com.example.db2lineage.model.RelationshipType.JOIN_ON,
            com.example.db2lineage.model.RelationshipType.MERGE_MATCH,
            com.example.db2lineage.model.RelationshipType.GROUP_BY,
            com.example.db2lineage.model.RelationshipType.ORDER_BY,
            com.example.db2lineage.model.RelationshipType.HAVING,
            com.example.db2lineage.model.RelationshipType.CONTROL_FLOW_CONDITION,
            com.example.db2lineage.model.RelationshipType.RETURN_VALUE
    );

    private record GroupKey(
            com.example.db2lineage.model.SourceObjectType sourceObjectType,
            String sourceObject,
            int lineNo
    ) {
        private static GroupKey from(RowDraft draft) {
            return new GroupKey(draft.sourceObjectType(), draft.sourceObject(), draft.lineNo());
        }
    }

    private record SemanticKey(
            com.example.db2lineage.model.SourceObjectType sourceObjectType,
            String sourceObject,
            String sourceField,
            com.example.db2lineage.model.TargetObjectType targetObjectType,
            String targetObject,
            String targetField,
            com.example.db2lineage.model.RelationshipType relationship,
            int lineNo,
            String lineContent,
            com.example.db2lineage.model.ConfidenceLevel confidence
    ) {
        private static SemanticKey from(RowDraft draft) {
            return new SemanticKey(
                    draft.sourceObjectType(),
                    draft.sourceObject(),
                    draft.sourceField(),
                    draft.targetObjectType(),
                    draft.targetObject(),
                    draft.targetField(),
                    draft.relationship(),
                    draft.lineNo(),
                    draft.lineContent(),
                    draft.confidence()
            );
        }
    }
}
