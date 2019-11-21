package com.salesforce.cantor.misc.metrics;

import com.salesforce.cantor.Cantor;
import com.salesforce.cantor.common.AbstractBaseSetsTest;

import java.io.IOException;

import static org.testng.Assert.*;

public class MetricCollectingSetsTest extends AbstractBaseSetsTest {
    @Override
    protected Cantor getCantor() throws IOException {
        return MetricCollectingTests.getCantor();
    }
}