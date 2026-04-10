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

        Map<GroupKey, List<RowDraft>> grouped = new LinkedHashMap<>();
        for (RowDraft draft : deduplicated.values()) {
            grouped.computeIfAbsent(GroupKey.from(draft), ignored -> new ArrayList<>()).add(draft);
        }

        List<RelationshipRow> finalized = new ArrayList<>();
        for (List<RowDraft> groupRows : grouped.values()) {
            groupRows.sort(Comparator
                    .comparingInt(RowDraft::statementOrder)
                    .thenComparingInt(RowDraft::naturalOrderOnLine)
                    .thenComparing(RowDraft::relationship)
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
