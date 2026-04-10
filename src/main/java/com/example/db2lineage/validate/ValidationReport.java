package com.example.db2lineage.validate;

import java.util.List;

public record ValidationReport(boolean valid, int rowCount, List<ValidationIssue> issues) {
    public ValidationReport {
        issues = List.copyOf(issues);
    }

    public String toHumanReadable() {
        StringBuilder builder = new StringBuilder();
        builder.append("Validation report for relationship_detail.tsv").append(System.lineSeparator());
        builder.append("  valid: ").append(valid).append(System.lineSeparator());
        builder.append("  rows: ").append(rowCount).append(System.lineSeparator());
        builder.append("  issues: ").append(issues.size()).append(System.lineSeparator());
        for (ValidationIssue issue : issues) {
            builder.append("  - [").append(issue.code()).append("] ").append(issue.message())
                    .append(System.lineSeparator());
        }
        return builder.toString();
    }
}
