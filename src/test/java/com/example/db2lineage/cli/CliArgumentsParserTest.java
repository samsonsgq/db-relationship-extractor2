package com.example.db2lineage.cli;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class CliArgumentsParserTest {

    @TempDir
    Path tempDir;

    @Test
    void parsesRequiredAndOptionalDirectoriesAsNormalizedAbsolutePaths() throws IOException {
        Path tableDir = Files.createDirectories(tempDir.resolve("tables"));
        Path viewDir = Files.createDirectories(tempDir.resolve("views"));
        Path functionDir = Files.createDirectories(tempDir.resolve("functions"));
        Path spDir = Files.createDirectories(tempDir.resolve("procedures"));
        Path outputDir = tempDir.resolve("out");
        Path extraDir = Files.createDirectories(tempDir.resolve("extra"));

        CliArgumentsParser parser = new CliArgumentsParser();
        CliArguments args = parser.parse(new String[]{
                "--tableDir", tableDir.toString(),
                "--viewDir", viewDir.toString(),
                "--functionDir", functionDir.toString(),
                "--spDir", spDir.toString(),
                "--outputDir", outputDir.toString(),
                "--extraDir", extraDir.toString()
        });

        assertEquals(tableDir.toAbsolutePath().normalize(), args.tableDir());
        assertEquals(viewDir.toAbsolutePath().normalize(), args.viewDir());
        assertEquals(functionDir.toAbsolutePath().normalize(), args.functionDir());
        assertEquals(spDir.toAbsolutePath().normalize(), args.spDir());
        assertEquals(outputDir.toAbsolutePath().normalize(), args.outputDir());
        assertTrue(args.extraDir().isPresent());
        assertEquals(extraDir.toAbsolutePath().normalize(), args.extraDir().get());
    }

    @Test
    void failsWhenRequiredArgumentIsMissing() throws IOException {
        Path tableDir = Files.createDirectories(tempDir.resolve("tables"));
        Path viewDir = Files.createDirectories(tempDir.resolve("views"));
        Path functionDir = Files.createDirectories(tempDir.resolve("functions"));

        CliArgumentsParser parser = new CliArgumentsParser();

        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () -> parser.parse(new String[]{
                "--tableDir", tableDir.toString(),
                "--viewDir", viewDir.toString(),
                "--functionDir", functionDir.toString(),
                "--outputDir", tempDir.resolve("out").toString()
        }));

        assertTrue(exception.getMessage().contains("Missing required argument: --spDir"));
    }

    @Test
    void failsWhenRequiredDirectoryDoesNotExist() throws IOException {
        Path tableDir = Files.createDirectories(tempDir.resolve("tables"));
        Path viewDir = Files.createDirectories(tempDir.resolve("views"));
        Path functionDir = Files.createDirectories(tempDir.resolve("functions"));

        CliArgumentsParser parser = new CliArgumentsParser();

        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () -> parser.parse(new String[]{
                "--tableDir", tableDir.toString(),
                "--viewDir", viewDir.toString(),
                "--functionDir", functionDir.toString(),
                "--spDir", tempDir.resolve("missing-procedures").toString(),
                "--outputDir", tempDir.resolve("out").toString()
        }));

        assertTrue(exception.getMessage().contains("Required directory does not exist for --spDir"));
    }

    @Test
    void optionalExtraDirIsNotRequired() throws IOException {
        CliArguments args = parseWithRequiredDirsOnly();
        assertFalse(args.extraDir().isPresent());
    }

    @Test
    void failsWhenOptionalExtraDirDoesNotExist() throws IOException {
        Path tableDir = Files.createDirectories(tempDir.resolve("tables"));
        Path viewDir = Files.createDirectories(tempDir.resolve("views"));
        Path functionDir = Files.createDirectories(tempDir.resolve("functions"));
        Path spDir = Files.createDirectories(tempDir.resolve("procedures"));

        CliArgumentsParser parser = new CliArgumentsParser();

        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () -> parser.parse(new String[]{
                "--tableDir", tableDir.toString(),
                "--viewDir", viewDir.toString(),
                "--functionDir", functionDir.toString(),
                "--spDir", spDir.toString(),
                "--outputDir", tempDir.resolve("out").toString(),
                "--extraDir", tempDir.resolve("missing-extra").toString()
        }));

        assertTrue(exception.getMessage().contains("Optional directory does not exist for --extraDir"));
    }

    @Test
    void createsOutputDirWhenMissing() throws IOException {
        Path tableDir = Files.createDirectories(tempDir.resolve("tables"));
        Path viewDir = Files.createDirectories(tempDir.resolve("views"));
        Path functionDir = Files.createDirectories(tempDir.resolve("functions"));
        Path spDir = Files.createDirectories(tempDir.resolve("procedures"));
        Path outputDir = tempDir.resolve("new-output");

        CliArgumentsParser parser = new CliArgumentsParser();
        CliArguments args = parser.parse(new String[]{
                "--tableDir", tableDir.toString(),
                "--viewDir", viewDir.toString(),
                "--functionDir", functionDir.toString(),
                "--spDir", spDir.toString(),
                "--outputDir", outputDir.toString()
        });

        assertTrue(Files.exists(outputDir));
        assertTrue(Files.isDirectory(outputDir));
        assertEquals(outputDir.toAbsolutePath().normalize(), args.outputDir());
    }

    @Test
    void failsOnUnknownArgumentAndIncludesUsage() throws IOException {
        Path tableDir = Files.createDirectories(tempDir.resolve("tables"));
        Path viewDir = Files.createDirectories(tempDir.resolve("views"));
        Path functionDir = Files.createDirectories(tempDir.resolve("functions"));
        Path spDir = Files.createDirectories(tempDir.resolve("procedures"));

        CliArgumentsParser parser = new CliArgumentsParser();

        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () -> parser.parse(new String[]{
                "--tableDir", tableDir.toString(),
                "--viewDir", viewDir.toString(),
                "--functionDir", functionDir.toString(),
                "--spDir", spDir.toString(),
                "--outputDir", tempDir.resolve("out").toString(),
                "--bogus", "x"
        }));

        assertTrue(exception.getMessage().contains("Unknown argument: --bogus"));
        assertTrue(exception.getMessage().contains("Usage:"));
    }

    @Test
    void failsOnDuplicateArgument() throws IOException {
        Path tableDir = Files.createDirectories(tempDir.resolve("tables"));
        Path viewDir = Files.createDirectories(tempDir.resolve("views"));
        Path functionDir = Files.createDirectories(tempDir.resolve("functions"));
        Path spDir = Files.createDirectories(tempDir.resolve("procedures"));

        CliArgumentsParser parser = new CliArgumentsParser();

        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () -> parser.parse(new String[]{
                "--tableDir", tableDir.toString(),
                "--viewDir", viewDir.toString(),
                "--viewDir", viewDir.toString(),
                "--functionDir", functionDir.toString(),
                "--spDir", spDir.toString(),
                "--outputDir", tempDir.resolve("out").toString()
        }));

        assertTrue(exception.getMessage().contains("Duplicate argument: --viewDir"));
    }

    @Test
    void failsOnEmptyRequiredArgumentValue() throws IOException {
        Path tableDir = Files.createDirectories(tempDir.resolve("tables"));
        Path viewDir = Files.createDirectories(tempDir.resolve("views"));
        Path functionDir = Files.createDirectories(tempDir.resolve("functions"));
        Path spDir = Files.createDirectories(tempDir.resolve("procedures"));

        CliArgumentsParser parser = new CliArgumentsParser();

        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () -> parser.parse(new String[]{
                "--tableDir", tableDir.toString(),
                "--viewDir", viewDir.toString(),
                "--functionDir", functionDir.toString(),
                "--spDir", spDir.toString(),
                "--outputDir", ""
        }));

        assertTrue(exception.getMessage().contains("Argument --outputDir must not be empty."));
    }

    private CliArguments parseWithRequiredDirsOnly() throws IOException {
        Path tableDir = Files.createDirectories(tempDir.resolve("tables"));
        Path viewDir = Files.createDirectories(tempDir.resolve("views"));
        Path functionDir = Files.createDirectories(tempDir.resolve("functions"));
        Path spDir = Files.createDirectories(tempDir.resolve("procedures"));

        CliArgumentsParser parser = new CliArgumentsParser();
        return parser.parse(new String[]{
                "--tableDir", tableDir.toString(),
                "--viewDir", viewDir.toString(),
                "--functionDir", functionDir.toString(),
                "--spDir", spDir.toString(),
                "--outputDir", tempDir.resolve("out").toString()
        });
    }
}
