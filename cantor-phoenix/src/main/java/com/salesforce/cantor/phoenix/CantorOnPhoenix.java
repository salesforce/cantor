package com.salesforce.cantor.phoenix;

import com.salesforce.cantor.*;

import javax.sql.DataSource;
import java.io.IOException;

public class CantorOnPhoenix implements Cantor {
    private final Events events;

    public CantorOnPhoenix(final DataSource datasource) throws IOException {
        this.events = new EventsOnPhoenix(datasource);
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
