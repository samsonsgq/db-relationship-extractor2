package com.example.db2lineage;

import com.example.db2lineage.cli.CliArguments;
import com.example.db2lineage.cli.CliArgumentsParser;
import com.example.db2lineage.emit.RelationshipDetailTsvWriter;
import com.example.db2lineage.model.RelationshipRow;

import java.nio.file.Path;
import java.util.Collections;
import java.util.List;

public final class RelationshipDetailMain {

    private RelationshipDetailMain() {
    }

    public static void main(String[] args) throws Exception {
        CliArgumentsParser parser = new CliArgumentsParser();
        CliArguments cliArguments;

        try {
            cliArguments = parser.parse(args);
        } catch (IllegalArgumentException e) {
            System.err.println("Invalid CLI arguments: " + e.getMessage());
            System.exit(1);
            return;
        }

        logStartup(cliArguments);

        // Phase 1 scaffold only: extraction is intentionally not implemented yet.
        List<RelationshipRow> rows = Collections.emptyList();
        Path outputFile = cliArguments.outputDir().resolve("relationship_detail.tsv");
        new RelationshipDetailTsvWriter().write(outputFile, rows);
    }

    private static void logStartup(CliArguments args) {
        System.out.println("Starting RelationshipDetailMain with directories:");
        System.out.println("  tableDir   : " + args.tableDir());
        System.out.println("  viewDir    : " + args.viewDir());
        System.out.println("  functionDir: " + args.functionDir());
        System.out.println("  spDir      : " + args.spDir());
        System.out.println("  outputDir  : " + args.outputDir());
        args.extraDir().ifPresent(path -> System.out.println("  extraDir   : " + path));
    }
}
