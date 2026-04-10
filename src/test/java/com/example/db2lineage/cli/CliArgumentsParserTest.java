package com.example.db2lineage.cli;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class CliArgumentsParserTest {

    @Test
    void parsesRequiredAndOptionalDirectories() {
        CliArgumentsParser parser = new CliArgumentsParser();
        CliArguments args = parser.parse(new String[]{
                "--tableDir", "tables",
                "--viewDir", "views",
                "--functionDir", "functions",
                "--spDir", "procedures",
                "--outputDir", "out",
                "--extraDir", "extra"
        });

        assertEquals("tables", args.tableDir().toString());
        assertEquals("views", args.viewDir().toString());
        assertEquals("functions", args.functionDir().toString());
        assertEquals("procedures", args.spDir().toString());
        assertEquals("out", args.outputDir().toString());
        assertTrue(args.extraDir().isPresent());
        assertEquals("extra", args.extraDir().get().toString());
    }
}
