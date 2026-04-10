package com.example.db2lineage;

import com.example.db2lineage.cli.CliArguments;
import com.example.db2lineage.cli.CliArgumentsParser;
import com.example.db2lineage.emit.RelationshipDetailTsvWriter;
import com.example.db2lineage.extract.ExtractionContext;
import com.example.db2lineage.extract.pipeline.ExtractionPipeline;
import com.example.db2lineage.model.RelationshipRow;
import com.example.db2lineage.parse.ParsedStatementResult;
import com.example.db2lineage.parse.SqlSourceFile;
import com.example.db2lineage.parse.SqlSourceFileLoader;
import com.example.db2lineage.parse.SqlStatementParser;
import com.example.db2lineage.parse.StatementSlice;
import com.example.db2lineage.parse.StatementSlicer;

import java.util.ArrayList;
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

        List<SqlSourceFile> sourceFiles = new SqlSourceFileLoader().load(cliArguments);
        System.out.println("Discovered SQL source files: " + sourceFiles.size());

        StatementSlicer slicer = new StatementSlicer();
        SqlStatementParser sqlStatementParser = new SqlStatementParser();
        List<ParsedStatementResult> parsedStatements = new ArrayList<>();

        for (SqlSourceFile sourceFile : sourceFiles) {
            List<StatementSlice> slices = slicer.slice(sourceFile);
            parsedStatements.addAll(sqlStatementParser.parseAll(slices));
        }

        long parsedCount = parsedStatements.stream().filter(result -> result.statement().isPresent()).count();
        long unsupportedCount = parsedStatements.stream().filter(ParsedStatementResult::unsupported).count();
        long parseFailedCount = parsedStatements.stream().filter(ParsedStatementResult::parseFailed).count();
        System.out.printf(
                "Parsed statement slices: total=%d, parsed=%d, unsupported=%d, parseFailed=%d%n",
                parsedStatements.size(),
                parsedCount,
                unsupportedCount,
                parseFailedCount
        );

        ExtractionContext extractionContext = new ExtractionContext(sourceFiles);
        List<RelationshipRow> rows = new ExtractionPipeline().extract(extractionContext, parsedStatements);
        new RelationshipDetailTsvWriter().writeToOutputDir(cliArguments.outputDir(), rows);
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
