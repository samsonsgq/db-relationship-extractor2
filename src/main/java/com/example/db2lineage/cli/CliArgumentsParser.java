package com.example.db2lineage.cli;

import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

public final class CliArgumentsParser {

    public CliArguments parse(String[] args) {
        if (args == null) {
            throw new IllegalArgumentException("Arguments must not be null.");
        }
        Map<String, String> values = parseFlags(args);

        return new CliArguments(
                requiredPath(values, "--tableDir"),
                requiredPath(values, "--viewDir"),
                requiredPath(values, "--functionDir"),
                requiredPath(values, "--spDir"),
                requiredPath(values, "--outputDir"),
                Optional.ofNullable(values.get("--extraDir")).map(Path::of)
        );
    }

    private Map<String, String> parseFlags(String[] args) {
        Map<String, String> values = new HashMap<>();
        for (int i = 0; i < args.length; i++) {
            String key = args[i];
            if (!key.startsWith("--")) {
                throw new IllegalArgumentException("Unexpected token: " + key);
            }
            if (i + 1 >= args.length || args[i + 1].startsWith("--")) {
                throw new IllegalArgumentException("Missing value for " + key);
            }
            String value = args[++i];
            values.put(key, value);
        }
        return values;
    }

    private Path requiredPath(Map<String, String> values, String key) {
        String value = values.get(key);
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException("Missing required argument " + key);
        }
        return Path.of(value);
    }
}
