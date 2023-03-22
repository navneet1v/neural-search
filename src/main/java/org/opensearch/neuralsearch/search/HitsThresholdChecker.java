/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.neuralsearch.search;

import org.apache.lucene.search.ScoreMode;

public class HitsThresholdChecker {
    private int hitCount;
    private final int totalHitsThreshold;

    HitsThresholdChecker(int totalHitsThreshold) {
        if (totalHitsThreshold < 0) {
            throw new IllegalArgumentException("totalHitsThreshold must be >= 0, got " + totalHitsThreshold);
        }
        this.totalHitsThreshold = totalHitsThreshold;
        assert totalHitsThreshold != Integer.MAX_VALUE;
    }

    int getHitsThreshold() {
        return totalHitsThreshold;
    }

    void incrementHitCount() {
        ++hitCount;
    }

    boolean isThresholdReached() {
        return hitCount > getHitsThreshold();
    }

    ScoreMode scoreMode() {
        return ScoreMode.TOP_SCORES;
    }

    public static HitsThresholdChecker create(final int totalHitsThreshold) {
        return new LocalHitsThresholdChecker(totalHitsThreshold);
    }

    static class LocalHitsThresholdChecker extends HitsThresholdChecker {
        private int hitCount;

        LocalHitsThresholdChecker(int totalHitsThreshold) {
            super(totalHitsThreshold);
            assert totalHitsThreshold != Integer.MAX_VALUE;
        }

        @Override
        void incrementHitCount() {
            ++hitCount;
        }

        @Override
        boolean isThresholdReached() {
            return hitCount > getHitsThreshold();
        }

        @Override
        ScoreMode scoreMode() {
            return ScoreMode.TOP_SCORES;
        }
    }
}
