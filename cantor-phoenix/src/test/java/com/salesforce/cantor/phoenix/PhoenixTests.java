package com.salesforce.cantor.phoenix;

import com.salesforce.cantor.Cantor;

import javax.sql.DataSource;
import java.io.IOException;

class PhoenixTests {
    private static final Cantor cantor;

    static {
        try {
            cantor = new CantorOnPhoenix(getDataSource());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    static Cantor getCantor() {
        return cantor;
    }

    private static DataSource getDataSource() {
        return PhoenixDataSourceProvider.getDatasource(new PhoenixDataSourceProperties());
    }
}
