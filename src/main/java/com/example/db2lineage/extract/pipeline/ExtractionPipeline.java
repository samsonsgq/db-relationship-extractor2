package com.example.db2lineage.extract.pipeline;

import com.example.db2lineage.extract.ExtractionContext;
import com.example.db2lineage.extract.RowCollector;
import com.example.db2lineage.model.RelationshipRow;
import com.example.db2lineage.parse.ParsedStatementResult;

import java.util.List;
import java.util.Objects;

public final class ExtractionPipeline {

    private final List<StatementExtractor> extractors;

    public ExtractionPipeline() {
        this(List.of(
                new SelectStatementExtractor(),
                new InsertStatementExtractor(),
                new UpdateStatementExtractor(),
                new DeleteStatementExtractor(),
                new MergeStatementExtractor(),
                new CreateViewStatementExtractor(),
                new CreateTableStatementExtractor(),
                new CreateFunctionStatementExtractor(),
                new CreateProcedureStatementExtractor(),
                new TruncateStatementExtractor(),
                new ExecuteStatementExtractor(),
                new UnknownStatementExtractor(),
                new ProceduralFallbackStatementExtractor()
        ));
    }

    ExtractionPipeline(List<StatementExtractor> extractors) {
        this.extractors = List.copyOf(extractors);
    }

    public List<RelationshipRow> extract(
            ExtractionContext context,
            List<ParsedStatementResult> parsedStatements
    ) {
        Objects.requireNonNull(context, "context must not be null");
        Objects.requireNonNull(parsedStatements, "parsedStatements must not be null");

        RowCollector collector = new RowCollector();
        for (ParsedStatementResult parsedStatement : parsedStatements) {
            StatementExtractor extractor = findExtractor(parsedStatement);
            extractor.extract(parsedStatement, context, collector);
        }

        return collector.finalizeRows();
    }

    private StatementExtractor findExtractor(ParsedStatementResult parsedStatement) {
        for (StatementExtractor extractor : extractors) {
            if (extractor.supports(parsedStatement)) {
                return extractor;
            }
        }
        throw new IllegalStateException("No statement extractor matched parsed statement result");
    }
}
