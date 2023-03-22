/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.neuralsearch.search.query;

import static org.opensearch.search.query.TopDocsCollectorContext.createTopDocsCollectorContext;

import java.io.IOException;
import java.util.LinkedList;
import java.util.function.Function;
import java.util.function.Supplier;

import lombok.extern.log4j.Log4j2;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TotalHits;
import org.opensearch.common.lucene.search.CompoundQueryTopDocs;
import org.opensearch.common.lucene.search.TopDocsAndMaxScore;
import org.opensearch.neuralsearch.query.CompoundQuery;
import org.opensearch.neuralsearch.search.CompoundTopScoreDocCollector;
import org.opensearch.neuralsearch.search.HitsThresholdChecker;
import org.opensearch.search.internal.ContextIndexSearcher;
import org.opensearch.search.internal.SearchContext;
import org.opensearch.search.query.QueryCollectorContext;
import org.opensearch.search.query.QueryPhase;
import org.opensearch.search.query.QuerySearchResult;
import org.opensearch.search.query.TopDocsCollectorContext;
import org.opensearch.search.rescore.RescoreContext;
import org.opensearch.search.sort.SortAndFormats;

@Log4j2
public class CompoundQueryPhaseSearcher extends QueryPhase.DefaultQueryPhaseSearcher {

    private Function<TopDocs[], TotalHits> totalHitsSupplier;
    private Supplier<TopDocs[]> topDocsSupplier;
    // private Supplier<Float> maxScoreSupplier;
    private Function<TopDocs[], Float> maxScoreSupplier;
    protected SortAndFormats sortAndFormats;

    public boolean searchWith(
        SearchContext searchContext,
        ContextIndexSearcher searcher,
        Query query,
        LinkedList<QueryCollectorContext> collectors,
        boolean hasFilterCollector,
        boolean hasTimeout
    ) throws IOException {
        if (query instanceof CompoundQuery) {
            return searchWithCollector(searchContext, searcher, query, collectors, hasFilterCollector, hasTimeout);
        }
        return super.searchWithCollector(searchContext, searcher, query, collectors, hasFilterCollector, hasTimeout);
    }

    protected boolean searchWithCollector(
        SearchContext searchContext,
        ContextIndexSearcher searcher,
        Query query,
        LinkedList<QueryCollectorContext> collectors,
        boolean hasFilterCollector,
        boolean hasTimeout
    ) throws IOException {
        log.info("searchWithCollector, shard " + searchContext.shardTarget().getShardId());

        final TopDocsCollectorContext topDocsFactory = createTopDocsCollectorContext(searchContext, hasFilterCollector);
        collectors.addFirst(topDocsFactory);

        IndexReader reader = searchContext.searcher().getIndexReader();
        //TODO This doesn't work if shard has no hits at all, must be 0. Copied from core, need to deep dive
        //int totalNumDocs = Math.max(1, reader.numDocs());
        int totalNumDocs = Math.max(0, reader.numDocs());
        if (searchContext.size() == 0) {
            throw new UnsupportedOperationException("Need to create no hits collector");
        }
        int numDocs = Math.min(searchContext.from() + searchContext.size(), totalNumDocs);
        final boolean rescore = !searchContext.rescore().isEmpty();
        if (rescore) {
            assert searchContext.sort() == null;
            for (RescoreContext rescoreContext : searchContext.rescore()) {
                numDocs = Math.max(numDocs, rescoreContext.getWindowSize());
            }
        }

        QuerySearchResult queryResult = searchContext.queryResult();

        // TODO Wrap collector into context
        CompoundTopScoreDocCollector collector = new CompoundTopScoreDocCollector(
            numDocs,
            HitsThresholdChecker.create(Math.max(numDocs, searchContext.trackTotalHitsUpTo())),
            null
        );
        topDocsSupplier = collector::topDocs;
        totalHitsSupplier = topDocs -> {
            // TopDocs[] topDocs = topDocsSupplier.get();
            // for now we do max from each sub-query result
            /*long numHits = 0;
            for (TopDocs td : topDocs) {
                numHits = Math.max(numHits, td.totalHits.value);
            }
            return new TotalHits(numHits, TotalHits.Relation.GREATER_THAN_OR_EQUAL_TO);*/
            return topDocs[0].totalHits;
            // return topDocs.get(topDocs.keySet().iterator().next()).totalHits;
        };// topDocsSupplier.get().totalHits;
        maxScoreSupplier = topDocs -> {
            // TopDocs[] topDocs = topDocsSupplier.get();
            // TODO review later, need to came up with valid max score
            if (topDocs.length == 0) {
                return Float.NaN;
            } else {
                // return topDocs.scoreDocs[0].score;
                TopDocs docs = topDocs[0];
                return docs.scoreDocs.length == 0 ? Float.NaN : docs.scoreDocs[0].score;
            }
        };
        sortAndFormats = searchContext.sort();

        searcher.search(query, collector);

        if (searchContext.terminateAfter() != SearchContext.DEFAULT_TERMINATE_AFTER && queryResult.terminatedEarly() == null) {
            queryResult.terminatedEarly(false);
        }

        postProcess(queryResult, collector);

        printQueryResults(searchContext, queryResult);

        return rescore;
    }

    private static void printQueryResults(SearchContext searchContext, QuerySearchResult queryResult) {
        StringBuilder sb = new StringBuilder();
        TopDocsAndMaxScore topDocsAndMaxScore = queryResult.topDocs();
        CompoundQueryTopDocs multiQueryTopDocs = (CompoundQueryTopDocs) topDocsAndMaxScore.topDocs;

        sb.append("Query result for shard ").append(searchContext.shardTarget().getShardId());
        sb.append("\n").append(multiQueryTopDocs.toString());

        log.info(sb.toString());
    }

    void postProcess(QuerySearchResult queryResult, CompoundTopScoreDocCollector collector) {
        final TopDocsAndMaxScore topDocs = newTopDocs(collector);
        queryResult.topDocs(topDocs, sortAndFormats == null ? null : sortAndFormats.formats);
    }

    TopDocsAndMaxScore newTopDocs(CompoundTopScoreDocCollector collector) {
        // TopDocs[] in = topDocsSupplier.get();
        TopDocs[] topDocs = collector.topDocs();
        float maxScore = maxScoreSupplier.apply(topDocs);
        /*if (in instanceof TopFieldDocs) {
            TopFieldDocs fieldDocs = (TopFieldDocs) in;
            newTopDocs = new TopFieldDocs(totalHitsSupplier.get(), fieldDocs.scoreDocs, fieldDocs.fields);
        } else {
            newTopDocs = new TopDocs(totalHitsSupplier.get(), in.scoreDocs);
        }*/
        final TopDocs newTopDocs = new CompoundQueryTopDocs(totalHitsSupplier.apply(topDocs), topDocs);
        return new TopDocsAndMaxScore(newTopDocs, maxScore);
    }
}
