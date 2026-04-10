package com.example.db2lineage.cli;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

public final class CliArgumentsParser {

    private static final Set<String> SUPPORTED_KEYS = Set.of(
            "--tableDir",
            "--viewDir",
            "--functionDir",
            "--spDir",
            "--outputDir",
            "--extraDir"
    );

    public CliArguments parse(String[] args) {
        if (args == null) {
            throw error("Arguments must not be null.");
        }

        Map<String, String> values = parseArgs(args);

        return new CliArguments(
                requiredExistingDir(values, "--tableDir"),
                requiredExistingDir(values, "--viewDir"),
                requiredExistingDir(values, "--functionDir"),
                requiredExistingDir(values, "--spDir"),
                requiredOutputDir(values, "--outputDir"),
                optionalExistingDir(values, "--extraDir")
        );
    }

    public String usageText() {
        return String.join(System.lineSeparator(),
                "Usage:",
                "  java ... RelationshipDetailMain --tableDir <dir> --viewDir <dir> --functionDir <dir> --spDir <dir> --outputDir <dir> [--extraDir <dir>]",
                "",
                "Required arguments:",
                "  --tableDir      Existing directory containing table SQL.",
                "  --viewDir       Existing directory containing view SQL.",
                "  --functionDir   Existing directory containing function SQL.",
                "  --spDir         Existing directory containing stored procedure SQL.",
                "  --outputDir     Output directory (created if it does not exist).",
                "",
                "Optional arguments:",
                "  --extraDir      Existing directory for extra SQL inputs."
        );
    }

    private Map<String, String> parseArgs(String[] args) {
        Map<String, String> values = new LinkedHashMap<>();

        for (int i = 0; i < args.length; i++) {
            String key = args[i];
            if (!key.startsWith("--")) {
                throw error("Unexpected token: " + key);
            }
            if (!SUPPORTED_KEYS.contains(key)) {
                throw error("Unknown argument: " + key);
            }
            if (values.containsKey(key)) {
                throw error("Duplicate argument: " + key);
            }

            if (i + 1 >= args.length || args[i + 1].startsWith("--")) {
                throw error("Missing value for argument: " + key);
            }

            String value = args[++i];
            if (value == null || value.isBlank()) {
                throw error("Argument " + key + " must not be empty.");
            }
            values.put(key, value);
        }

        return values;
    }

    private Path requiredExistingDir(Map<String, String> values, String key) {
        String raw = values.get(key);
        if (raw == null || raw.isBlank()) {
            throw error("Missing required argument: " + key);
        }

        Path dir = normalize(raw);
        if (!Files.exists(dir)) {
            throw error("Required directory does not exist for " + key + ": " + dir);
        }
        if (!Files.isDirectory(dir)) {
            throw error("Required path is not a directory for " + key + ": " + dir);
        }
        return dir;
    }

    private Path requiredOutputDir(Map<String, String> values, String key) {
        String raw = values.get(key);
        if (raw == null || raw.isBlank()) {
            throw error("Missing required argument: " + key);
        }

        Path outputDir = normalize(raw);
        if (Files.exists(outputDir) && !Files.isDirectory(outputDir)) {
            throw error("Output path is not a directory for " + key + ": " + outputDir);
        }

        try {
            Files.createDirectories(outputDir);
        } catch (Exception e) {
            throw error("Unable to create output directory for " + key + ": " + outputDir);
        }

        return outputDir;
    }

    private Optional<Path> optionalExistingDir(Map<String, String> values, String key) {
        String raw = values.get(key);
        if (raw == null) {
            return Optional.empty();
        }
        if (raw.isBlank()) {
            throw error("Argument " + key + " must not be empty.");
        }

        Path dir = normalize(raw);
        if (!Files.exists(dir)) {
            throw error("Optional directory does not exist for " + key + ": " + dir);
        }
        if (!Files.isDirectory(dir)) {
            throw error("Optional path is not a directory for " + key + ": " + dir);
        }
        return Optional.of(dir);
    }

    private Path normalize(String pathValue) {
        return Path.of(pathValue).toAbsolutePath().normalize();
    }

    private IllegalArgumentException error(String message) {
        return new IllegalArgumentException(message + System.lineSeparator() + System.lineSeparator() + usageText());
    }
}
