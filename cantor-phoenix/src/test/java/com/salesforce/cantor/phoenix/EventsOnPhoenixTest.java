package com.salesforce.cantor.phoenix;

import com.salesforce.cantor.Cantor;
import com.salesforce.cantor.common.AbstractBaseEventsTest;

import java.io.IOException;

public class EventsOnPhoenixTest extends AbstractBaseEventsTest {
    @Override
    public Cantor getCantor() throws IOException {
        return PhoenixTests.getCantor();
    }
}
