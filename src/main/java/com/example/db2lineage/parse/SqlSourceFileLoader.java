package com.example.db2lineage.parse;

import com.example.db2lineage.cli.CliArguments;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Stream;

public final class SqlSourceFileLoader {

    private static final Set<String> SUPPORTED_EXTENSIONS = Set.of(".sql", ".ddl", ".sqll");

    public List<SqlSourceFile> load(CliArguments args) {
        List<SourceRoot> sourceRoots = new ArrayList<>();
        sourceRoots.add(new SourceRoot(SqlSourceCategory.TABLE_DIR, args.tableDir()));
        sourceRoots.add(new SourceRoot(SqlSourceCategory.VIEW_DIR, args.viewDir()));
        sourceRoots.add(new SourceRoot(SqlSourceCategory.FUNCTION_DIR, args.functionDir()));
        sourceRoots.add(new SourceRoot(SqlSourceCategory.SP_DIR, args.spDir()));
        args.extraDir().ifPresent(path -> sourceRoots.add(new SourceRoot(SqlSourceCategory.EXTRA_DIR, path)));
        return load(sourceRoots, false);
    }

    List<SqlSourceFile> load(List<SourceRoot> sourceRoots, boolean includeHiddenFiles) {
        List<SqlSourceFile> result = new ArrayList<>();
        Set<Path> seenRealPaths = new HashSet<>();

        for (SourceRoot sourceRoot : sourceRoots) {
            for (Path filePath : scanRoot(sourceRoot.root(), includeHiddenFiles)) {
                Path realPath = toRealPath(filePath);
                if (!seenRealPaths.add(realPath)) {
                    continue;
                }

                Path relativePath = sourceRoot.root().relativize(filePath).normalize();
                result.add(new SqlSourceFile(
                        sourceRoot.category(),
                        filePath.toAbsolutePath().normalize(),
                        relativePath,
                        readFullText(filePath),
                        readRawLines(filePath)
                ));
            }
        }

        return List.copyOf(result);
    }

    private List<Path> scanRoot(Path root, boolean includeHiddenFiles) {
        try (Stream<Path> stream = Files.walk(root)) {
            return stream
                    .filter(Files::isRegularFile)
                    .filter(path -> includeHiddenFiles || !hasHiddenSegment(root.relativize(path)))
                    .filter(this::hasSupportedExtension)
                    .sorted(Comparator.comparing(path -> normalizeForOrdering(root.relativize(path))))
                    .toList();
        } catch (IOException e) {
            throw new IllegalStateException("Failed to scan SQL files under root: " + root, e);
        }
    }

    private String normalizeForOrdering(Path relativePath) {
        return relativePath.normalize().toString().replace('\\', '/');
    }

    private boolean hasHiddenSegment(Path relativePath) {
        for (Path segment : relativePath) {
            if (segment.toString().startsWith(".")) {
                return true;
            }
        }
        return false;
    }

    private boolean hasSupportedExtension(Path filePath) {
        String name = Optional.ofNullable(filePath.getFileName())
                .map(Path::toString)
                .orElse("");
        String lower = name.toLowerCase();
        return SUPPORTED_EXTENSIONS.stream().anyMatch(lower::endsWith);
    }

    private String readFullText(Path filePath) {
        try {
            return Files.readString(filePath, StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new IllegalStateException("Failed to read SQL file text: " + filePath, e);
        }
    }

    private List<String> readRawLines(Path filePath) {
        try {
            return Files.readAllLines(filePath, StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new IllegalStateException("Failed to read SQL file lines: " + filePath, e);
        }
    }

    private Path toRealPath(Path filePath) {
        try {
            return filePath.toRealPath();
        } catch (IOException e) {
            throw new IllegalStateException("Failed to resolve real path for SQL file: " + filePath, e);
        }
    }

    record SourceRoot(SqlSourceCategory category, Path root) {
    }
}
