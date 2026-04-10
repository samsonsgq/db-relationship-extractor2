package com.example.db2lineage.validate;

import com.example.db2lineage.emit.RelationshipDetailTsvWriter;
import com.example.db2lineage.model.ConfidenceLevel;
import com.example.db2lineage.model.RelationshipType;
import com.example.db2lineage.parse.SqlSourceFile;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public final class RelationshipDetailValidator {

    public ValidationReport validate(Path tsvFile, List<SqlSourceFile> sourceFiles) {
        List<ValidationIssue> issues = new ArrayList<>();
        List<TsvRow> rows = readRows(tsvFile, issues);

        validateDuplicates(rows, issues);
        validateEnums(rows, issues);
        validateFieldPopulation(rows, issues);
        validateLineNumbers(rows, issues);
        validateLineSeq(rows, issues);
        validateLineContentMatch(rows, sourceFiles, issues);
        validateInsertTargetCol(rows, issues);
        validateSelectFieldLiteral(rows, issues);
        validateMixedCallableTargets(rows, issues);

        return new ValidationReport(issues.isEmpty(), rows.size(), issues);
    }

    private List<TsvRow> readRows(Path file, List<ValidationIssue> issues) {
        List<String> lines;
        try {
            lines = Files.readAllLines(file, StandardCharsets.UTF_8);
        } catch (IOException e) {
            issues.add(new ValidationIssue("READ_ERROR", "Unable to read TSV: " + file + " (" + e.getMessage() + ")"));
            return List.of();
        }
        if (lines.isEmpty()) {
            issues.add(new ValidationIssue("EMPTY_FILE", "TSV is empty."));
            return List.of();
        }

        if (!RelationshipDetailTsvWriter.HEADER.equals(lines.get(0))) {
            issues.add(new ValidationIssue("HEADER_MISMATCH", "TSV header does not exactly match expected header."));
        }

        List<TsvRow> rows = new ArrayList<>();
        for (int i = 1; i < lines.size(); i++) {
            String raw = lines.get(i);
            String[] cols = raw.split("\\t", -1);
            if (cols.length != 11) {
                issues.add(new ValidationIssue("COLUMN_COUNT", "Line " + (i + 1) + " has " + cols.length + " columns; expected 11."));
                continue;
            }
            rows.add(new TsvRow(i + 1, raw, cols));
        }
        return rows;
    }

    private void validateDuplicates(List<TsvRow> rows, List<ValidationIssue> issues) {
        Set<String> seen = new HashSet<>();
        for (TsvRow row : rows) {
            if (!seen.add(row.rawLine)) {
                issues.add(new ValidationIssue("DUPLICATE_ROW", "Duplicate row at TSV line " + row.tsvLineNo + "."));
            }
        }
    }

    private void validateEnums(List<TsvRow> rows, List<ValidationIssue> issues) {
        for (TsvRow row : rows) {
            if (!isEnumValue(row.relationship(), RelationshipType.class)) {
                issues.add(new ValidationIssue("INVALID_RELATIONSHIP", "Invalid relationship '" + row.relationship() + "' at TSV line " + row.tsvLineNo + "."));
            }
            if (!isEnumValue(row.confidence(), ConfidenceLevel.class)) {
                issues.add(new ValidationIssue("INVALID_CONFIDENCE", "Invalid confidence '" + row.confidence() + "' at TSV line " + row.tsvLineNo + "."));
            }
        }
    }

    private void validateFieldPopulation(List<TsvRow> rows, List<ValidationIssue> issues) {
        Set<String> objectLevel = Set.of(
                "CREATE_VIEW", "CREATE_TABLE", "CREATE_PROCEDURE", "CREATE_FUNCTION",
                "SELECT_TABLE", "SELECT_VIEW", "INSERT_TABLE", "UPDATE_TABLE", "MERGE_INTO", "DELETE_TABLE",
                "DELETE_VIEW", "TRUNCATE_TABLE", "CALL_FUNCTION", "CALL_PROCEDURE", "CTE_DEFINE", "CTE_READ",
                "UNION_INPUT", "CURSOR_DEFINE", "CURSOR_READ", "DYNAMIC_SQL_EXEC", "UNKNOWN"
        );

        for (TsvRow row : rows) {
            if (objectLevel.contains(row.relationship())) {
                if (!row.sourceField().isEmpty() || !row.targetField().isEmpty()) {
                    issues.add(new ValidationIssue("INVALID_FIELD_POPULATION", "Object-level relationship has populated field columns at TSV line " + row.tsvLineNo + "."));
                }
            }
        }
    }

    private void validateLineNumbers(List<TsvRow> rows, List<ValidationIssue> issues) {
        for (TsvRow row : rows) {
            try {
                int lineNo = Integer.parseInt(row.lineNo());
                if (lineNo < 1) {
                    issues.add(new ValidationIssue("INVALID_LINE_NO", "line_no must be >= 1 at TSV line " + row.tsvLineNo + "."));
                }
            } catch (NumberFormatException e) {
                issues.add(new ValidationIssue("INVALID_LINE_NO", "line_no is not an integer at TSV line " + row.tsvLineNo + "."));
            }
            try {
                int lineSeq = Integer.parseInt(row.lineRelationSeq());
                if (lineSeq < 0) {
                    issues.add(new ValidationIssue("INVALID_LINE_RELATION_SEQ", "line_relation_seq must be >= 0 at TSV line " + row.tsvLineNo + "."));
                }
            } catch (NumberFormatException e) {
                issues.add(new ValidationIssue("INVALID_LINE_RELATION_SEQ", "line_relation_seq is not an integer at TSV line " + row.tsvLineNo + "."));
            }
        }
    }

    private void validateLineSeq(List<TsvRow> rows, List<ValidationIssue> issues) {
        Map<String, List<Integer>> byLineGroup = new HashMap<>();
        for (TsvRow row : rows) {
            if (!isInteger(row.lineRelationSeq())) {
                continue;
            }
            byLineGroup.computeIfAbsent(row.sourceObjectType() + "|" + row.sourceObject() + "|" + row.lineNo(), key -> new ArrayList<>())
                    .add(Integer.parseInt(row.lineRelationSeq()));
        }

        for (Map.Entry<String, List<Integer>> entry : byLineGroup.entrySet()) {
            List<Integer> seqs = entry.getValue().stream().sorted().toList();
            for (int i = 0; i < seqs.size(); i++) {
                if (seqs.get(i) != i) {
                    issues.add(new ValidationIssue("NON_CONTIGUOUS_LINE_RELATION_SEQ", "line_relation_seq is not contiguous from 0 for group " + entry.getKey() + "."));
                    break;
                }
            }
        }
    }

    private void validateLineContentMatch(List<TsvRow> rows, List<SqlSourceFile> sourceFiles, List<ValidationIssue> issues) {
        Map<String, List<SqlSourceFile>> byObject = new HashMap<>();
        for (SqlSourceFile file : sourceFiles) {
            String fileName = file.absolutePath().getFileName().toString();
            String object = stripExtension(fileName).toUpperCase();
            byObject.computeIfAbsent(object, ignored -> new ArrayList<>()).add(file);
        }

        for (TsvRow row : rows) {
            if (!isInteger(row.lineNo())) {
                continue;
            }
            int lineNo = Integer.parseInt(row.lineNo());
            List<SqlSourceFile> candidates = byObject.getOrDefault(row.sourceObject().toUpperCase(), List.of());
            if (candidates.isEmpty()) {
                issues.add(new ValidationIssue("SOURCE_OBJECT_FILE_MISSING", "Could not map source_object to file for TSV line " + row.tsvLineNo + ": " + row.sourceObject()));
                continue;
            }
            boolean matched = false;
            for (SqlSourceFile file : candidates) {
                if (lineNo <= file.rawLines().size() && file.rawLines().get(lineNo - 1).equals(row.lineContent())) {
                    matched = true;
                    break;
                }
            }
            if (!matched) {
                issues.add(new ValidationIssue("LINE_CONTENT_MISMATCH", "line_content mismatch at TSV line " + row.tsvLineNo + " for source_object=" + row.sourceObject() + ", line_no=" + lineNo + "."));
            }
        }
    }

    private void validateInsertTargetCol(List<TsvRow> rows, List<ValidationIssue> issues) {
        for (TsvRow row : rows) {
            if (!"INSERT_TARGET_COL".equals(row.relationship())) {
                continue;
            }
            String upper = row.lineContent().toUpperCase();
            int idxInsert = upper.indexOf("INSERT INTO");
            if (idxInsert < 0) {
                continue;
            }
            int idxValues = upper.indexOf(" VALUES");
            int idxSelect = upper.indexOf(" SELECT");
            int end = idxValues >= 0 ? idxValues : (idxSelect >= 0 ? idxSelect : upper.length());
            String head = upper.substring(idxInsert, end);
            if (!head.contains("(") || !head.contains(")")) {
                issues.add(new ValidationIssue("INSERT_TARGET_COL_WITHOUT_TARGET_LIST", "INSERT_TARGET_COL emitted without explicit target column list at TSV line " + row.tsvLineNo + "."));
            }
        }
    }

    private void validateSelectFieldLiteral(List<TsvRow> rows, List<ValidationIssue> issues) {
        for (TsvRow row : rows) {
            if ("SELECT_FIELD".equals(row.relationship()) && row.sourceField().startsWith("CONSTANT:")) {
                issues.add(new ValidationIssue("SELECT_FIELD_LITERAL_PROJECTION", "SELECT_FIELD emitted for literal projection at TSV line " + row.tsvLineNo + "."));
            }
        }
    }

    private void validateMixedCallableTargets(List<TsvRow> rows, List<ValidationIssue> issues) {
        Map<String, Set<String>> callableTargets = new HashMap<>();
        for (TsvRow row : rows) {
            if (!"FUNCTION_PARAM_MAP".equals(row.relationship()) && !"CALL_PARAM_MAP".equals(row.relationship())) {
                continue;
            }
            if ("UNKNOWN".equalsIgnoreCase(row.targetObjectType())) {
                continue;
            }
            callableTargets.computeIfAbsent(row.sourceObject() + "|" + row.targetObject(), ignored -> new HashSet<>())
                    .add(classifyTargetField(row.targetField()));
        }

        for (Map.Entry<String, Set<String>> entry : callableTargets.entrySet()) {
            if (entry.getValue().contains("NAMED") && entry.getValue().contains("POSITIONAL")) {
                issues.add(new ValidationIssue("MIXED_CALLABLE_PARAM_TARGET_STYLE", "Mixed named and positional parameter targets for callable " + entry.getKey() + "."));
            }
        }
    }

    private String classifyTargetField(String targetField) {
        if (targetField == null || targetField.isBlank()) {
            return "UNKNOWN";
        }
        return targetField.startsWith("$") ? "POSITIONAL" : "NAMED";
    }

    private boolean isInteger(String value) {
        try {
            Integer.parseInt(value);
            return true;
        } catch (NumberFormatException e) {
            return false;
        }
    }

    private static <E extends Enum<E>> boolean isEnumValue(String raw, Class<E> enumClass) {
        try {
            Enum.valueOf(enumClass, raw);
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    private String stripExtension(String fileName) {
        int idx = fileName.lastIndexOf('.');
        return idx > 0 ? fileName.substring(0, idx) : fileName;
    }

    private static final class TsvRow {
        private final int tsvLineNo;
        private final String rawLine;
        private final String[] columns;

        private TsvRow(int tsvLineNo, String rawLine, String[] columns) {
            this.tsvLineNo = tsvLineNo;
            this.rawLine = rawLine;
            this.columns = columns;
        }

        private String sourceObjectType() { return columns[0]; }
        private String sourceObject() { return columns[1]; }
        private String sourceField() { return columns[2]; }
        private String targetObjectType() { return columns[3]; }
        private String targetObject() { return columns[4]; }
        private String targetField() { return columns[5]; }
        private String relationship() { return columns[6]; }
        private String lineNo() { return columns[7]; }
        private String lineRelationSeq() { return columns[8]; }
        private String lineContent() { return columns[9]; }
        private String confidence() { return columns[10]; }
    }
}
