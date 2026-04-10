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
        CliArguments cliArguments = new CliArgumentsParser().parse(args);

        // Scaffold only: extraction is intentionally not implemented yet.
        List<RelationshipRow> rows = Collections.emptyList();
        Path outputFile = cliArguments.outputDir().resolve("relationship_detail.tsv");
        new RelationshipDetailTsvWriter().write(outputFile, rows);
    }
}
