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

package com.amazon.opendistro.elasticsearch.performanceanalyzer.config;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.config.overrides.ConfigOverrides;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.Arrays;
import java.util.List;

public class ConfigOverridesTestHelper {
    private static final ObjectMapper MAPPER = new ObjectMapper();
    public static final String RCA1 = "rca1";
    public static final String RCA2 = "rca2";
    public static final String RCA3 = "rca3";
    public static final String RCA4 = "rca4";
    public static final String ACTION1 = "act1";
    public static final String ACTION2 = "act2";
    public static final String ACTION3 = "act3";
    public static final String ACTION4 = "act4";
    public static final String DECIDER1 = "dec1";
    public static final String DECIDER2 = "dec2";
    public static final String DECIDER3 = "dec3";
    public static final String DECIDER4 = "dec4";
    public static final List<String> DISABLED_RCAS_LIST = Arrays.asList(RCA1, RCA2);
    public static final List<String> ENABLED_RCAS_LIST = Arrays.asList(RCA3, RCA4);
    public static final List<String> DISABLED_ACTIONS_LIST = Arrays.asList(ACTION1, ACTION2);
    public static final List<String> ENABLED_DECIDERS_LIST = Arrays.asList(DECIDER3, DECIDER4);

    public static ConfigOverrides buildValidConfigOverrides() {
        ConfigOverrides overrides = new ConfigOverrides();
        overrides.getDisable().setRcas(DISABLED_RCAS_LIST);
        overrides.getDisable().setActions(DISABLED_ACTIONS_LIST);
        overrides.getEnable().setRcas(ENABLED_RCAS_LIST);
        overrides.getEnable().setDeciders(ENABLED_DECIDERS_LIST);

        return overrides;
    }

    public static String getValidConfigOverridesJson() throws JsonProcessingException {
        return MAPPER.writeValueAsString(buildValidConfigOverrides());
    }
}
