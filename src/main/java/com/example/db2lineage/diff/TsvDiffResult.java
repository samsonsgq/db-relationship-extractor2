package com.example.db2lineage.diff;

import java.util.List;

public record TsvDiffResult(boolean identical, int missingCount, int unexpectedCount, List<String> missingRows, List<String> unexpectedRows) {
    public TsvDiffResult {
        missingRows = List.copyOf(missingRows);
        unexpectedRows = List.copyOf(unexpectedRows);
    }

    public String toHumanReadable() {
        StringBuilder builder = new StringBuilder();
        builder.append("TSV diff report").append(System.lineSeparator())
                .append("  identical: ").append(identical).append(System.lineSeparator())
                .append("  missingRows: ").append(missingCount).append(System.lineSeparator())
                .append("  unexpectedRows: ").append(unexpectedCount).append(System.lineSeparator());
        for (String row : missingRows) {
            builder.append("  - missing: ").append(row).append(System.lineSeparator());
        }
        for (String row : unexpectedRows) {
            builder.append("  - unexpected: ").append(row).append(System.lineSeparator());
        }
        return builder.toString();
    }
}
