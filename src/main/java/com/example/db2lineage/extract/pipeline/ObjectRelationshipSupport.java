package com.example.db2lineage.extract.pipeline;

import com.example.db2lineage.extract.ExtractionContext;
import com.example.db2lineage.model.ConfidenceLevel;
import com.example.db2lineage.model.RelationshipType;
import com.example.db2lineage.model.RowDraft;
import com.example.db2lineage.model.SourceObjectType;
import com.example.db2lineage.model.TargetObjectType;
import com.example.db2lineage.parse.ParsedStatementResult;
import com.example.db2lineage.parse.SqlSourceCategory;
import com.example.db2lineage.parse.StatementSlice;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.FromItem;
import net.sf.jsqlparser.statement.select.Join;
import net.sf.jsqlparser.statement.select.ParenthesedFromItem;
import net.sf.jsqlparser.statement.select.ParenthesedSelect;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.SetOperationList;
import net.sf.jsqlparser.statement.select.WithItem;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

final class ObjectRelationshipSupport {

    static final String UNKNOWN_DYNAMIC_SQL = "UNKNOWN_DYNAMIC_SQL";
    static final String UNKNOWN_UNSUPPORTED_STATEMENT = "UNKNOWN_UNSUPPORTED_STATEMENT";
    static final String UNKNOWN_UNRESOLVED_OBJECT = "UNKNOWN_UNRESOLVED_OBJECT";

    private static final Pattern CREATE_PROCEDURE_NAME = Pattern.compile("(?i)\\bCREATE\\s+(?:OR\\s+REPLACE\\s+)?PROCEDURE\\s+([A-Z0-9_.$\\\"]+)");
    private static final Pattern CREATE_FUNCTION_NAME = Pattern.compile("(?i)\\bCREATE\\s+(?:OR\\s+REPLACE\\s+)?FUNCTION\\s+([A-Z0-9_.$\\\"]+)");
    private static final Pattern CREATE_VIEW_NAME = Pattern.compile("(?i)\\bCREATE\\s+(?:OR\\s+REPLACE\\s+)?VIEW\\s+([A-Z0-9_.$\\\"]+)");
    private static final Pattern CREATE_TABLE_NAME = Pattern.compile("(?i)\\bCREATE\\s+TABLE\\s+([A-Z0-9_.$\\\"]+)");
    private static final Pattern EXECUTE_IMMEDIATE_TOKEN = Pattern.compile("(?i)\\bEXECUTE\\s+IMMEDIATE\\s+([^;]+)");

    private ObjectRelationshipSupport() {
    }

    static RowDraft objectLevelDraft(
            ExtractionContext context,
            ParsedStatementResult parsedStatement,
            RelationshipType relationship,
            TargetObjectType targetObjectType,
            String targetObject,
            int naturalOrderOnLine
    ) {
        StatementSlice slice = parsedStatement.slice();
        return new RowDraft(
                sourceObjectType(slice),
                sourceObjectName(slice),
                "",
                targetObjectType,
                normalizeObjectName(targetObject),
                "",
                relationship,
                slice.startLine(),
                firstLine(slice),
                ConfidenceLevel.PARSER,
                statementOrder(context, parsedStatement),
                naturalOrderOnLine
        );
    }

    static SourceObjectType sourceObjectType(StatementSlice slice) {
        SqlSourceCategory category = slice.sourceCategory();
        if (category == SqlSourceCategory.VIEW_DIR) {
            return SourceObjectType.VIEW_DDL;
        }
        if (category == SqlSourceCategory.FUNCTION_DIR) {
            return SourceObjectType.FUNCTION;
        }
        if (category == SqlSourceCategory.SP_DIR) {
            return SourceObjectType.PROCEDURE;
        }
        return SourceObjectType.SCRIPT;
    }

    static String sourceObjectName(StatementSlice slice) {
        String statementText = slice.statementText();
        Matcher procMatcher = CREATE_PROCEDURE_NAME.matcher(statementText);
        if (procMatcher.find()) {
            return normalizeObjectName(procMatcher.group(1));
        }

        Matcher functionMatcher = CREATE_FUNCTION_NAME.matcher(statementText);
        if (functionMatcher.find()) {
            return normalizeObjectName(functionMatcher.group(1));
        }
        Matcher viewMatcher = CREATE_VIEW_NAME.matcher(statementText);
        if (viewMatcher.find()) {
            return normalizeObjectName(viewMatcher.group(1));
        }
        Matcher tableMatcher = CREATE_TABLE_NAME.matcher(statementText);
        if (tableMatcher.find()) {
            return normalizeObjectName(tableMatcher.group(1));
        }
        String fullText = slice.sourceFile().fullText();
        if (slice.sourceCategory() == SqlSourceCategory.SP_DIR) {
            Matcher procInFile = CREATE_PROCEDURE_NAME.matcher(fullText);
            if (procInFile.find()) {
                return normalizeObjectName(procInFile.group(1));
            }
        }
        if (slice.sourceCategory() == SqlSourceCategory.FUNCTION_DIR) {
            Matcher fnInFile = CREATE_FUNCTION_NAME.matcher(fullText);
            if (fnInFile.find()) {
                return normalizeObjectName(fnInFile.group(1));
            }
        }

        String relative = slice.sourceFile().relativePath().toString().replace('\\', '/');
        if (relative.isBlank()) {
            return slice.sourceFile().absolutePath().toString();
        }
        int slash = relative.lastIndexOf('/');
        String fileName = slash >= 0 ? relative.substring(slash + 1) : relative;
        int dot = fileName.lastIndexOf('.');
        String stem = dot > 0 ? fileName.substring(0, dot) : fileName;
        return stem.toUpperCase(Locale.ROOT);
    }

    static int statementOrder(ExtractionContext context, ParsedStatementResult parsedStatement) {
        StatementSlice slice = parsedStatement.slice();
        int sourceIndex = context.sourceFiles().indexOf(slice.sourceFile());
        if (sourceIndex < 0) {
            sourceIndex = stablePositiveHash(slice.sourceFile().relativePath().toString().replace('\\', '/'));
        }
        return sourceIndex * 10_000 + slice.ordinalWithinFile();
    }

    static String firstLine(StatementSlice slice) {
        return LineAnchorResolver.statementStart(slice, 0).lineContent();
    }

    static String normalizeObjectName(String objectName) {
        if (objectName == null) {
            return UNKNOWN_UNRESOLVED_OBJECT;
        }
        String normalized = objectName.trim();
        if (normalized.isEmpty()) {
            return UNKNOWN_UNRESOLVED_OBJECT;
        }
        return normalized;
    }

    static List<TableRef> collectSelectReadObjects(Select select) {
        LinkedHashSet<TableRef> ordered = new LinkedHashSet<>();
        collectSelectReadsInternal(select, ordered);
        return List.copyOf(ordered);
    }

    static List<String> collectCteNames(Select select) {
        List<WithItem<?>> withItems = select.getWithItemsList();
        if (withItems == null || withItems.isEmpty()) {
            return List.of();
        }

        List<String> names = new ArrayList<>();
        for (WithItem<?> withItem : withItems) {
            String alias = withItem.getAliasName();
            if (alias != null && !alias.isBlank()) {
                names.add(alias.trim());
            }
        }
        return List.copyOf(names);
    }

    static void collectFromItemObjects(FromItem fromItem, Set<TableRef> ordered) {
        if (fromItem == null) {
            return;
        }

        if (fromItem instanceof Table table) {
            String object = normalizeObjectName(table.getFullyQualifiedName());
            ordered.add(new TableRef(object, TargetObjectType.TABLE));
            return;
        }

        if (fromItem instanceof Select nestedSelect) {
            collectSelectReadsInternal(nestedSelect, ordered);
            return;
        }

        if (fromItem instanceof ParenthesedFromItem parenthesedFromItem) {
            collectFromItemObjects(parenthesedFromItem.getFromItem(), ordered);
            if (parenthesedFromItem.getJoins() != null) {
                for (Join join : parenthesedFromItem.getJoins()) {
                    collectFromItemObjects(join.getFromItem(), ordered);
                }
            }
            return;
        }

        if (fromItem instanceof ParenthesedSelect parenthesedSelect) {
            collectSelectReadsInternal(parenthesedSelect, ordered);
        }
    }

    static String extractDynamicSqlSourceToken(String statementText) {
        Matcher matcher = EXECUTE_IMMEDIATE_TOKEN.matcher(statementText);
        if (!matcher.find()) {
            return "";
        }

        String token = matcher.group(1).trim();
        return token.replaceAll("[;\\s]+$", "");
    }

    private static void collectSelectReadsInternal(Select select, Set<TableRef> ordered) {
        if (select == null) {
            return;
        }

        List<WithItem<?>> withItems = select.getWithItemsList();
        if (withItems != null) {
            for (WithItem<?> withItem : withItems) {
                if (withItem.getSelect() != null) {
                    collectSelectReadsInternal(withItem.getSelect(), ordered);
                }
            }
        }

        if (select instanceof PlainSelect plainSelect) {
            collectFromItemObjects(plainSelect.getFromItem(), ordered);
            if (plainSelect.getJoins() != null) {
                for (Join join : plainSelect.getJoins()) {
                    collectFromItemObjects(join.getFromItem(), ordered);
                }
            }
            return;
        }

        if (select instanceof SetOperationList setOperationList && setOperationList.getSelects() != null) {
            for (Select nested : setOperationList.getSelects()) {
                collectSelectReadsInternal(nested, ordered);
            }
        }
    }

    private static int stablePositiveHash(String value) {
        return Math.abs(Objects.requireNonNullElse(value, "").toLowerCase(Locale.ROOT).hashCode() % 1_000_000);
    }

    record TableRef(String objectName, TargetObjectType targetType) {
    }
}
