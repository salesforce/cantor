package com.salesforce.cantor.metrics;

import com.salesforce.cantor.Cantor;
import com.salesforce.cantor.common.AbstractBaseEventsTest;

import java.io.IOException;

public class MetricCollectingEventsTest extends AbstractBaseEventsTest {
    @Override
    protected Cantor getCantor() throws IOException {
        return MetricCollectingTests.getCantor();
    }
}