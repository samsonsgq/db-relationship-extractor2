package com.example.db2lineage.diff;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public final class RelationshipDetailTsvDiffer {

    public TsvDiffResult diff(Path expected, Path actual) {
        List<String> expectedLines = read(expected);
        List<String> actualLines = read(actual);

        List<String> expectedRows = expectedLines.size() > 1 ? expectedLines.subList(1, expectedLines.size()) : List.of();
        List<String> actualRows = actualLines.size() > 1 ? actualLines.subList(1, actualLines.size()) : List.of();

        Map<String, Integer> expectedCounts = toCounts(expectedRows);
        Map<String, Integer> actualCounts = toCounts(actualRows);

        List<String> missing = new ArrayList<>();
        List<String> unexpected = new ArrayList<>();

        for (Map.Entry<String, Integer> entry : expectedCounts.entrySet()) {
            int actualCount = actualCounts.getOrDefault(entry.getKey(), 0);
            for (int i = actualCount; i < entry.getValue(); i++) {
                missing.add(entry.getKey());
            }
        }

        for (Map.Entry<String, Integer> entry : actualCounts.entrySet()) {
            int expectedCount = expectedCounts.getOrDefault(entry.getKey(), 0);
            for (int i = expectedCount; i < entry.getValue(); i++) {
                unexpected.add(entry.getKey());
            }
        }

        return new TsvDiffResult(missing.isEmpty() && unexpected.isEmpty(), missing.size(), unexpected.size(), missing, unexpected);
    }

    private List<String> read(Path file) {
        try {
            return Files.readAllLines(file, StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new IllegalStateException("Unable to read TSV file for diff: " + file, e);
        }
    }

    private Map<String, Integer> toCounts(List<String> rows) {
        Map<String, Integer> counts = new HashMap<>();
        for (String row : rows) {
            counts.merge(row, 1, Integer::sum);
        }
        return counts;
    }
}
