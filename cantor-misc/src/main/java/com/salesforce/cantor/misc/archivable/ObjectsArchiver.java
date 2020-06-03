package com.salesforce.cantor.misc.archivable;

import com.salesforce.cantor.Objects;

import java.io.IOException;

public interface ObjectsArchiver {
    void archive(final Objects objects, final String namespace) throws IOException;

    void restore(final Objects objects, final String namespace) throws IOException;
}
