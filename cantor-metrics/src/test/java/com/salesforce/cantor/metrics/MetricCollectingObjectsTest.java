package com.salesforce.cantor.metrics;

import com.salesforce.cantor.Cantor;
import com.salesforce.cantor.common.AbstractBaseObjectsTest;

import java.io.IOException;

import static org.testng.Assert.*;

public class MetricCollectingObjectsTest extends AbstractBaseObjectsTest {

    @Override
    protected Cantor getCantor() throws IOException {
        return MetricCollectingTests.getCantor();
    }
}