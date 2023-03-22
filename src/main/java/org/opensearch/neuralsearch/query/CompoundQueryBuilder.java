/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.neuralsearch.query;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Objects;

import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.experimental.Accessors;
import lombok.extern.log4j.Log4j2;

import org.apache.lucene.search.Query;
import org.opensearch.common.ParsingException;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;
import org.opensearch.common.lucene.search.Queries;
import org.opensearch.core.ParseField;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.index.query.AbstractQueryBuilder;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.QueryRewriteContext;
import org.opensearch.index.query.QueryShardContext;
import org.opensearch.index.query.QueryShardException;
import org.opensearch.index.query.Rewriteable;

@Log4j2
@Getter
@Setter
@Accessors(chain = true, fluent = true)
@NoArgsConstructor
public class CompoundQueryBuilder extends AbstractQueryBuilder<CompoundQueryBuilder> {
    public static final String NAME = "compound";

    private static final ParseField QUERIES_FIELD = new ParseField("queries");

    private final List<QueryBuilder> queries = new ArrayList<>();

    private String fieldName;

    public CompoundQueryBuilder(StreamInput in) throws IOException {
        super(in);
        queries.addAll(readQueries(in));
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        writeQueries(out, queries);
    }

    /**
     * Add one sub-query
     * @param queryBuilder
     * @return
     */
    public CompoundQueryBuilder add(QueryBuilder queryBuilder) {
        if (queryBuilder == null) {
            throw new IllegalArgumentException("inner dismax query clause cannot be null");
        }
        queries.add(queryBuilder);
        return this;
    }

    public List<QueryBuilder> innerQueries() {
        return this.queries;
    }

    @Override
    protected void doXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(NAME);
        builder.startArray(QUERIES_FIELD.getPreferredName());
        for (QueryBuilder queryBuilder : queries) {
            queryBuilder.toXContent(builder, params);
        }
        builder.endArray();
        printBoostAndQueryName(builder);
        builder.endObject();
    }

    @Override
    protected Query doToQuery(QueryShardContext queryShardContext) throws IOException {
        Collection<Query> queryCollection = toQueries(queries, queryShardContext);
        if (queryCollection.isEmpty()) {
            return Queries.newMatchNoDocsQuery("no clauses for compound query.");
        }

        return new CompoundQuery(queryCollection);
    }

    public static CompoundQueryBuilder fromXContent(XContentParser parser) throws IOException {
        float boost = AbstractQueryBuilder.DEFAULT_BOOST;

        final List<QueryBuilder> queries = new ArrayList<>();
        boolean queriesFound = false;
        String queryName = null;

        String currentFieldName = null;
        XContentParser.Token token;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (token == XContentParser.Token.START_OBJECT) {
                if (QUERIES_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    queriesFound = true;
                    queries.add(parseInnerQueryBuilder(parser));
                } else {
                    throw new ParsingException(parser.getTokenLocation(), "[dis_max] query does not support [" + currentFieldName + "]");
                }
            } else if (token == XContentParser.Token.START_ARRAY) {
                if (QUERIES_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    queriesFound = true;
                    while (token != XContentParser.Token.END_ARRAY) {
                        queries.add(parseInnerQueryBuilder(parser));
                        token = parser.nextToken();
                    }
                } else {
                    throw new ParsingException(parser.getTokenLocation(), "[dis_max] query does not support [" + currentFieldName + "]");
                }
            } else {
                if (AbstractQueryBuilder.BOOST_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    boost = parser.floatValue();
                } else if (AbstractQueryBuilder.NAME_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    queryName = parser.text();
                } else {
                    throw new ParsingException(parser.getTokenLocation(), "[dis_max] query does not support [" + currentFieldName + "]");
                }
            }
        }

        if (!queriesFound) {
            throw new ParsingException(parser.getTokenLocation(), "[dis_max] requires 'queries' field with at least one clause");
        }

        CompoundQueryBuilder compoundQueryBuilder = new CompoundQueryBuilder();
        compoundQueryBuilder.queryName(queryName);
        compoundQueryBuilder.boost(boost);
        for (QueryBuilder query : queries) {
            compoundQueryBuilder.add(query);
        }
        return compoundQueryBuilder;
    }

    protected QueryBuilder doRewrite(QueryRewriteContext queryShardContext) throws IOException {
        CompoundQueryBuilder newBuilder = new CompoundQueryBuilder();
        boolean changed = false;
        for (QueryBuilder query : queries) {
            QueryBuilder result = query.rewrite(queryShardContext);
            if (result != query) {
                changed = true;
            }
            newBuilder.add(result);
        }
        if (changed) {
            newBuilder.queryName(queryName);
            newBuilder.boost(boost);
            return newBuilder;
        } else {
            return this;
        }
    }

    @Override
    protected boolean doEquals(CompoundQueryBuilder compoundQueryBuilder) {
        return false;
    }

    @Override
    protected int doHashCode() {
        return Objects.hash(queries);
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    static List<QueryBuilder> readQueries(StreamInput in) throws IOException {
        int size = in.readVInt();
        List<QueryBuilder> queries = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            queries.add(in.readNamedWriteable(QueryBuilder.class));
        }
        return queries;
    }

    static void writeQueries(StreamOutput out, List<? extends QueryBuilder> queries) throws IOException {
        out.writeVInt(queries.size());
        for (QueryBuilder query : queries) {
            out.writeNamedWriteable(query);
        }
    }

    static Collection<Query> toQueries(Collection<QueryBuilder> queryBuilders, QueryShardContext context) throws QueryShardException,
        IOException {
        List<Query> queries = new ArrayList<>(queryBuilders.size());
        for (QueryBuilder queryBuilder : queryBuilders) {
            Query query = Rewriteable.rewrite(queryBuilder, context).toQuery(context);
            if (query != null) {
                queries.add(query);
            }
        }
        return queries;
    }
}
