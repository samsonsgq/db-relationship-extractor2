package com.example.db2lineage.parse;

import java.nio.file.Path;
import java.util.List;

public record SqlSourceFile(
        SqlSourceCategory sourceCategory,
        Path absolutePath,
        Path relativePath,
        String fullText,
        List<String> rawLines
) {
    public SqlSourceFile {
        rawLines = List.copyOf(rawLines);
    }

    public String getRawLine(int oneBasedLineNo) {
        if (oneBasedLineNo < 1 || oneBasedLineNo > rawLines.size()) {
            throw new IllegalArgumentException("Line number out of range: " + oneBasedLineNo);
        }
        return rawLines.get(oneBasedLineNo - 1);
    }

    public int getLineCount() {
        return rawLines.size();
    }
}
