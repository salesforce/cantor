package com.salesforce.cantor.metrics;

import com.salesforce.cantor.Cantor;
import com.salesforce.cantor.common.AbstractBaseMapsTest;
import com.salesforce.cantor.common.AbstractBaseObjectsTest;

import java.io.IOException;

import static org.testng.Assert.*;

public class MetricCollectingMapsTest extends AbstractBaseMapsTest {

    @Override
    protected Cantor getCantor() throws IOException {
        return MetricCollectingTests.getCantor();
    }
}