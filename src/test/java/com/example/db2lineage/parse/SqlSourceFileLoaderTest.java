package com.example.db2lineage.parse;

import com.example.db2lineage.cli.CliArguments;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;

class SqlSourceFileLoaderTest {

    @TempDir
    Path tempDir;

    @Test
    void loadsFilesWithExactRawSourceAndCategoryTagging() throws IOException {
        Path tableDir = Files.createDirectories(tempDir.resolve("tables"));
        Path viewDir = Files.createDirectories(tempDir.resolve("views"));
        Path functionDir = Files.createDirectories(tempDir.resolve("functions"));
        Path spDir = Files.createDirectories(tempDir.resolve("sps"));
        Path extraDir = Files.createDirectories(tempDir.resolve("extra"));

        String tableSql = "CREATE TABLE T1 (ID INT);\n\n-- comment with trailing spaces   \nINSERT INTO T1 VALUES (1);\n";
        Path tableFile = writeFile(tableDir.resolve("schema/table_01.sql"), tableSql);
        writeFile(viewDir.resolve("v_01.sql"), "CREATE VIEW V1 AS SELECT 1 AS X;\n");
        writeFile(functionDir.resolve("f_01.ddl"), "CREATE FUNCTION F1() RETURNS INT RETURN 1;\n");
        writeFile(spDir.resolve("sp_01.sql"), "CREATE PROCEDURE P1() LANGUAGE SQL BEGIN END;\n");
        writeFile(extraDir.resolve("notes.sql"), "VALUES 1;\n");

        CliArguments cli = new CliArguments(tableDir, viewDir, functionDir, spDir, tempDir.resolve("out"), Optional.of(extraDir));

        List<SqlSourceFile> files = new SqlSourceFileLoader().load(cli);

        assertEquals(5, files.size());

        SqlSourceFile loadedTable = files.stream()
                .filter(f -> f.absolutePath().equals(tableFile.toAbsolutePath().normalize()))
                .findFirst()
                .orElseThrow();

        assertEquals(SqlSourceCategory.TABLE_DIR, loadedTable.sourceCategory());
        assertEquals(Path.of("schema/table_01.sql"), loadedTable.relativePath());
        assertEquals(tableSql, loadedTable.fullText());
        assertEquals(4, loadedTable.getLineCount());
        assertEquals("", loadedTable.getRawLine(2));
        assertEquals("-- comment with trailing spaces   ", loadedTable.getRawLine(3));
        assertEquals("INSERT INTO T1 VALUES (1);", loadedTable.getRawLine(4));
    }

    @Test
    void supportsOneBasedLineLookupAndValidation() {
        SqlSourceFile sourceFile = new SqlSourceFile(
                SqlSourceCategory.EXTRA_DIR,
                Path.of("/tmp/a.sql"),
                Path.of("a.sql"),
                "line1\nline2\n",
                List.of("line1", "line2")
        );

        assertEquals("line1", sourceFile.getRawLine(1));
        assertEquals("line2", sourceFile.getRawLine(2));
        assertThrows(IllegalArgumentException.class, () -> sourceFile.getRawLine(0));
        assertThrows(IllegalArgumentException.class, () -> sourceFile.getRawLine(3));
    }

    @Test
    void scansRecursivelyIgnoresHiddenAndFiltersExtensions() throws IOException {
        Path tableDir = Files.createDirectories(tempDir.resolve("tables"));
        Path viewDir = Files.createDirectories(tempDir.resolve("views"));
        Path functionDir = Files.createDirectories(tempDir.resolve("functions"));
        Path spDir = Files.createDirectories(tempDir.resolve("sps"));

        writeFile(tableDir.resolve("a.sql"), "VALUES 1;\n");
        writeFile(tableDir.resolve("nested/b.ddl"), "VALUES 2;\n");
        writeFile(tableDir.resolve("nested/c.txt"), "SHOULD_NOT_LOAD\n");
        writeFile(tableDir.resolve(".hidden.sql"), "SHOULD_NOT_LOAD\n");
        writeFile(tableDir.resolve(".hidden-dir/d.sql"), "SHOULD_NOT_LOAD\n");

        CliArguments cli = new CliArguments(tableDir, viewDir, functionDir, spDir, tempDir.resolve("out"), Optional.empty());
        List<SqlSourceFile> files = new SqlSourceFileLoader().load(cli);

        List<String> relativePaths = files.stream().map(f -> f.relativePath().toString().replace('\\', '/')).toList();
        assertEquals(List.of("a.sql", "nested/b.ddl"), relativePaths);
        assertFalse(relativePaths.contains("nested/c.txt"));
        assertFalse(relativePaths.contains(".hidden.sql"));
    }

    @Test
    void providesStableDeterministicOrderAcrossDirectoriesAndRelativePaths() throws IOException {
        Path tableDir = Files.createDirectories(tempDir.resolve("tables"));
        Path viewDir = Files.createDirectories(tempDir.resolve("views"));
        Path functionDir = Files.createDirectories(tempDir.resolve("functions"));
        Path spDir = Files.createDirectories(tempDir.resolve("sps"));
        Path extraDir = Files.createDirectories(tempDir.resolve("extra"));

        writeFile(tableDir.resolve("z.sql"), "VALUES 1;\n");
        writeFile(tableDir.resolve("a/a.sql"), "VALUES 1;\n");
        writeFile(viewDir.resolve("v.sql"), "VALUES 1;\n");
        writeFile(functionDir.resolve("f.sql"), "VALUES 1;\n");
        writeFile(spDir.resolve("s.sql"), "VALUES 1;\n");
        writeFile(extraDir.resolve("e.sql"), "VALUES 1;\n");

        CliArguments cli = new CliArguments(tableDir, viewDir, functionDir, spDir, tempDir.resolve("out"), Optional.of(extraDir));
        List<SqlSourceFile> files = new SqlSourceFileLoader().load(cli);

        assertEquals(List.of(
                "TABLE_DIR:a/a.sql",
                "TABLE_DIR:z.sql",
                "VIEW_DIR:v.sql",
                "FUNCTION_DIR:f.sql",
                "SP_DIR:s.sql",
                "EXTRA_DIR:e.sql"
        ), files.stream().map(f -> f.sourceCategory() + ":" + f.relativePath().toString().replace('\\', '/')).toList());
    }

    @Test
    void suppressesDuplicatePhysicalFilesFromOverlappingRoots() throws IOException {
        Path parent = Files.createDirectories(tempDir.resolve("shared"));
        Path child = Files.createDirectories(parent.resolve("extra"));
        Path viewDir = Files.createDirectories(tempDir.resolve("views"));
        Path functionDir = Files.createDirectories(tempDir.resolve("functions"));
        Path spDir = Files.createDirectories(tempDir.resolve("sps"));

        Path duplicateFile = writeFile(child.resolve("dup.sql"), "VALUES 1;\n");

        CliArguments cli = new CliArguments(parent, viewDir, functionDir, spDir, tempDir.resolve("out"), Optional.of(child));
        List<SqlSourceFile> files = new SqlSourceFileLoader().load(cli);

        long dupCount = files.stream()
                .filter(f -> f.absolutePath().equals(duplicateFile.toAbsolutePath().normalize()))
                .count();

        assertEquals(1, dupCount);
        assertEquals(SqlSourceCategory.TABLE_DIR, files.stream()
                .filter(f -> f.absolutePath().equals(duplicateFile.toAbsolutePath().normalize()))
                .findFirst().orElseThrow().sourceCategory());
    }

    private Path writeFile(Path path, String content) throws IOException {
        Files.createDirectories(path.getParent());
        return Files.writeString(path, content);
    }
}
