package com.example.db2lineage.util;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;

public final class SourceTraversalOrder {

    private SourceTraversalOrder() {
    }

    public static List<Path> orderedDirectories(
            Path viewDir,
            Path functionDir,
            Path spDir,
            Path tableDir,
            Optional<Path> extraDir
    ) {
        List<Path> ordered = new ArrayList<>();
        ordered.add(viewDir);
        ordered.add(functionDir);
        ordered.add(spDir);
        ordered.add(tableDir);
        extraDir.ifPresent(ordered::add);
        return ordered;
    }

    public static Comparator<Path> pathComparator() {
        return Comparator.comparing(Path::toString);
    }
}
