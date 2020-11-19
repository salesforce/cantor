package com.salesforce.cantor.common.credentials;

import com.salesforce.cantor.management.CantorCredentials;

import java.io.*;
import java.util.Scanner;

public class CsvFileCantorCredentials implements CantorCredentials {
    private final String accessKey;
    private final String secretKey;

    public CsvFileCantorCredentials(final File secretFile) throws FileNotFoundException {
        try (final Scanner csvReader = new Scanner(new FileInputStream(secretFile))) {
            csvReader.useDelimiter(",");

            this.accessKey = csvReader.next();
            this.secretKey = csvReader.next();
        }
    }

    @Override
    public String getAccessKey() {
        return this.accessKey;
    }

    @Override
    public String getSecretKey() {
        return this.secretKey;
    }
}
