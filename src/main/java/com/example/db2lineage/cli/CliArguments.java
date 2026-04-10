package com.example.db2lineage.cli;

import java.nio.file.Path;
import java.util.Optional;

public record CliArguments(
        Path tableDir,
        Path viewDir,
        Path functionDir,
        Path spDir,
        Path outputDir,
        Optional<Path> extraDir,
        CliMode mode,
        Optional<Path> expectedOutputDir,
        boolean failOnValidationError
) {
}
