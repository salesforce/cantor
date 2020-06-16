package com.salesforce.cantor.phoenix;

import com.salesforce.cantor.*;

import java.io.IOException;
import java.sql.Connection;

public class CantorOnPhoenix implements Cantor {
    private final Events events;

    public CantorOnPhoenix(final String url) throws IOException {
        this.events = new EventsOnPhoenix(url);
    }

    @Override
    public Events events() {
        return this.events;
    }

    @Override
    public Objects objects() {
        throw new UnsupportedOperationException("Objects not implemented on Phoenix");
    }

    @Override
    public Sets sets() {
        throw new UnsupportedOperationException("Sets are not implemented on Phoenix");
    }
}
