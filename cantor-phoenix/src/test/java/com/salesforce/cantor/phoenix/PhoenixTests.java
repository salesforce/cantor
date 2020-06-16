package com.salesforce.cantor.phoenix;

import com.salesforce.cantor.Cantor;

import javax.sql.DataSource;
import java.io.IOException;

public class PhoenixTests {
    private static final Cantor cantor;

    static {
        try {
            cantor = new CantorOnPhoenix("jdbc:phoenix:localhost");
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    static Cantor getCantor() throws IOException {
        return cantor;
    }
}
