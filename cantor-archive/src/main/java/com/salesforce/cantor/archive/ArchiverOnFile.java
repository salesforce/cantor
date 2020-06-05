package com.salesforce.cantor.archive;

import com.salesforce.cantor.misc.archivable.CantorArchiver;
import com.salesforce.cantor.misc.archivable.EventsArchiver;
import com.salesforce.cantor.misc.archivable.ObjectsArchiver;
import com.salesforce.cantor.misc.archivable.SetsArchiver;

public class ArchiverOnFile implements CantorArchiver {
    @Override
    public SetsArchiver sets() {
        return new SetsArchiverOnFile();
    }

    @Override
    public ObjectsArchiver objects() {
        return new ObjectsArchiverOnFile();
    }

    @Override
    public EventsArchiver events() {
        return new EventsArchiverOnFile();
    }
}
