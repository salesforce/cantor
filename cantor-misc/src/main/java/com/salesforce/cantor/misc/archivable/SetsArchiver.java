package com.salesforce.cantor.misc.archivable;

import com.salesforce.cantor.Sets;
import com.salesforce.cantor.misc.archivable.impl.ArchivableEvents;

import java.io.IOException;

/**
 * SetsArchiver is the contract used by {@link ArchivableEvents} when handling archiving of {@link Sets}
 */
public interface SetsArchiver {
    /**
     *  Checks for the existence of any archives for the given set, and
     *  will result in {@code restore()} being called if true
     *  <br><br>
     *  Depending on the implementation {@code restore()} could be called even if the set doesn't exist, given
     *  the namespace does.
     */
    boolean hasArchives(String namespace, String set);

    /**
     * Will retrieve and archive the provided set in a namespace.
     */
    default void archive(Sets sets, String namespace, String set) throws IOException {
        archive(sets, namespace);
    }

    /**
     * Will retrieve and archive all sets in a namespace.
     */
    void archive(Sets sets, String namespace) throws IOException;

    /**
     * Will restore an archived set for this namespace by name.
     * <br><br>
     * {@code restore()} by set is not guaranteed to restore only the target set. It may restore up to the entire
     * rest of the archived namespace.
     * <br><br>
     * It will depend on the implementation
     */
    default void restore(Sets sets, String namespace, String set) throws IOException {
        restore(sets, namespace);
    }

    /**
     * Will restore all archived sets for this namespace.
     */
    void restore(Sets sets, String namespace) throws IOException;
}
