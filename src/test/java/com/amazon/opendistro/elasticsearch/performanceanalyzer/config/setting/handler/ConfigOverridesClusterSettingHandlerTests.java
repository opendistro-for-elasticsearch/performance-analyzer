/*
 * Copyright <2020> Amazon.com, Inc. or its affiliates. All Rights Reserved.
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

package com.amazon.opendistro.elasticsearch.performanceanalyzer.config.setting.handler;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.config.ConfigOverridesTestHelper;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.config.overrides.ConfigOverrides;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.config.overrides.ConfigOverridesWrapper;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.config.setting.ClusterSettingsManager;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.elasticsearch.common.settings.Setting;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;

import static com.amazon.opendistro.elasticsearch.performanceanalyzer.config.ConfigOverridesTestHelper.ACTION1;
import static com.amazon.opendistro.elasticsearch.performanceanalyzer.config.ConfigOverridesTestHelper.ACTION2;
import static com.amazon.opendistro.elasticsearch.performanceanalyzer.config.ConfigOverridesTestHelper.DECIDER1;
import static com.amazon.opendistro.elasticsearch.performanceanalyzer.config.ConfigOverridesTestHelper.DECIDER2;
import static com.amazon.opendistro.elasticsearch.performanceanalyzer.config.ConfigOverridesTestHelper.DECIDER3;
import static com.amazon.opendistro.elasticsearch.performanceanalyzer.config.ConfigOverridesTestHelper.DECIDER4;
import static com.amazon.opendistro.elasticsearch.performanceanalyzer.config.ConfigOverridesTestHelper.RCA1;
import static com.amazon.opendistro.elasticsearch.performanceanalyzer.config.ConfigOverridesTestHelper.RCA2;
import static com.amazon.opendistro.elasticsearch.performanceanalyzer.config.ConfigOverridesTestHelper.RCA3;
import static com.amazon.opendistro.elasticsearch.performanceanalyzer.config.ConfigOverridesTestHelper.RCA4;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.MockitoAnnotations.initMocks;

public class ConfigOverridesClusterSettingHandlerTests {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final String TEST_KEY = "test key";
    private static final ConfigOverrides EMPTY_OVERRIDES = new ConfigOverrides();
    private ConfigOverridesClusterSettingHandler testClusterSettingHandler;
    private ConfigOverridesWrapper testOverridesWrapper;
    private Setting<String> testSetting;
    private ConfigOverrides testOverrides;

    @Mock
    private ClusterSettingsManager mockClusterSettingsManager;

    @Captor
    private ArgumentCaptor<String> updatedClusterSettingCaptor;

    @Before
    public void setUp() {
        initMocks(this);
        this.testSetting = Setting.simpleString(TEST_KEY);
        this.testOverridesWrapper = new ConfigOverridesWrapper();
        this.testOverrides = ConfigOverridesTestHelper.buildValidConfigOverrides();
        testOverridesWrapper.setCurrentClusterConfigOverrides(EMPTY_OVERRIDES);

        this.testClusterSettingHandler = new ConfigOverridesClusterSettingHandler(
                testOverridesWrapper, mockClusterSettingsManager, testSetting);
    }

    @Test
    public void onSettingUpdateSuccessTest() throws JsonProcessingException {
        String updatedSettingValue = ConfigOverridesTestHelper.getValidConfigOverridesJson();
        testClusterSettingHandler.onSettingUpdate(updatedSettingValue);

        assertEquals(updatedSettingValue, MAPPER.writeValueAsString(testOverridesWrapper.getCurrentClusterConfigOverrides()));
    }

    @Test
    public void onSettingUpdateFailureTest() throws IOException {
        String updatedSettingValue = "invalid json";
        ConfigOverridesWrapper failingOverridesWrapper = new ConfigOverridesWrapper();

        testClusterSettingHandler = new ConfigOverridesClusterSettingHandler(
                failingOverridesWrapper, mockClusterSettingsManager, testSetting);

        testClusterSettingHandler.onSettingUpdate(updatedSettingValue);

        assertEquals(MAPPER.writeValueAsString(EMPTY_OVERRIDES),
                MAPPER.writeValueAsString(testOverridesWrapper.getCurrentClusterConfigOverrides()));
    }

    @Test
    public void onSettingUpdateEmptySettingsTest() throws IOException {
        ConfigOverridesWrapper failingOverridesWrapper = new ConfigOverridesWrapper();

        testClusterSettingHandler = new ConfigOverridesClusterSettingHandler(
                failingOverridesWrapper, mockClusterSettingsManager, testSetting);

        testClusterSettingHandler.onSettingUpdate(null);

        assertEquals(MAPPER.writeValueAsString(EMPTY_OVERRIDES),
                MAPPER.writeValueAsString(testOverridesWrapper.getCurrentClusterConfigOverrides()));
    }

    @Test
    public void updateConfigOverridesMergeSuccessTest() throws IOException {
        testOverridesWrapper.setCurrentClusterConfigOverrides(testOverrides);

        ConfigOverrides expectedOverrides = new ConfigOverrides();
        ConfigOverrides additionalOverrides = new ConfigOverrides();
        // current enabled rcas: 3,4. current disabled rcas: 1,2
        additionalOverrides.getEnable().setRcas(Arrays.asList(RCA1, RCA1));

        expectedOverrides.getEnable().setRcas(Arrays.asList(RCA1, RCA3, RCA4));
        expectedOverrides.getDisable().setRcas(Collections.singletonList(RCA2));

        // current enabled deciders: 3,4. current disabled deciders: none
        additionalOverrides.getDisable().setDeciders(Arrays.asList(DECIDER3, DECIDER1));
        additionalOverrides.getEnable().setDeciders(Collections.singletonList(DECIDER2));

        expectedOverrides.getEnable().setDeciders(Arrays.asList(DECIDER2, DECIDER4));
        expectedOverrides.getDisable().setDeciders(Arrays.asList(DECIDER3, DECIDER1));

        // current enabled actions: none. current disabled actions: 1,2
        additionalOverrides.getEnable().setActions(Arrays.asList(ACTION1, ACTION2));

        expectedOverrides.getEnable().setActions(Arrays.asList(ACTION1, ACTION2));

        testClusterSettingHandler.updateConfigOverrides(additionalOverrides);
        verify(mockClusterSettingsManager).updateSetting(eq(testSetting), updatedClusterSettingCaptor.capture());

        assertTrue(areEqual(expectedOverrides, MAPPER.readValue(updatedClusterSettingCaptor.getValue(), ConfigOverrides.class)));
    }

    private boolean areEqual(final ConfigOverrides expected, final ConfigOverrides actual) {
        Collections.sort(expected.getEnable().getRcas());
        Collections.sort(actual.getEnable().getRcas());
        assertEquals(expected.getEnable().getRcas(), actual.getEnable().getRcas());

        Collections.sort(expected.getEnable().getActions());
        Collections.sort(actual.getEnable().getActions());
        assertEquals(expected.getEnable().getActions(), actual.getEnable().getActions());

        Collections.sort(expected.getEnable().getDeciders());
        Collections.sort(actual.getEnable().getDeciders());
        assertEquals(expected.getEnable().getDeciders(), actual.getEnable().getDeciders());

        Collections.sort(expected.getDisable().getRcas());
        Collections.sort(actual.getDisable().getRcas());
        assertEquals(expected.getDisable().getRcas(), actual.getDisable().getRcas());

        Collections.sort(expected.getDisable().getActions());
        Collections.sort(actual.getDisable().getActions());
        assertEquals(expected.getDisable().getActions(), actual.getDisable().getActions());

        Collections.sort(expected.getDisable().getDeciders());
        Collections.sort(actual.getDisable().getDeciders());
        assertEquals(expected.getDisable().getDeciders(), actual.getDisable().getDeciders());

        return true;
    }
}