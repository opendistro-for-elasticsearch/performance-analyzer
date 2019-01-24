/*
 * Copyright <2019> Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

package com.amazon.opendistro.performanceanalyzer.listener;

import org.elasticsearch.search.internal.SearchContext;

interface SearchListener {
    default void preQueryPhase(SearchContext searchContext) { }
    default void queryPhase(SearchContext searchContext, long tookInNanos) { }
    default void failedQueryPhase(SearchContext searchContext) { }
    default void preFetchPhase(SearchContext searchContext) { }
    default void fetchPhase(SearchContext searchContext, long tookInNanos) { }
    default void failedFetchPhase(SearchContext searchContext) { }
}
