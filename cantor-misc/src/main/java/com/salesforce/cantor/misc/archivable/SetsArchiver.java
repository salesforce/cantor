package com.salesforce.cantor.misc.archivable;

import com.salesforce.cantor.Sets;

import java.io.IOException;

public interface SetsArchiver {
    void archive(final Sets sets, final String namespace) throws IOException;

    void restore(final Sets sets, final String namespace) throws IOException;
}
