/*
Copyright (c) 2014, Colorado State University
All rights reserved.

Redistribution and use in source and binary forms, with or without modification,
are permitted provided that the following conditions are met:

1. Redistributions of source code must retain the above copyright notice, this
   list of conditions and the following disclaimer.
2. Redistributions in binary form must reproduce the above copyright notice,
   this list of conditions and the following disclaimer in the documentation
   and/or other materials provided with the distribution.

This software is provided by the copyright holders and contributors "as is" and
any express or implied warranties, including, but not limited to, the implied
warranties of merchantability and fitness for a particular purpose are
disclaimed. In no event shall the copyright holder or contributors be liable for
any direct, indirect, incidental, special, exemplary, or consequential damages
(including, but not limited to, procurement of substitute goods or services;
loss of use, data, or profits; or business interruption) however caused and on
any theory of liability, whether in contract, strict liability, or tort
(including negligence or otherwise) arising in any way out of the use of this
software, even if advised of the possibility of such damage.
*/

package galileo.fs;

import java.io.IOException;

import galileo.dataset.Block;
import galileo.dataset.Metadata;
import galileo.serialization.SerializationException;

/**
 * Defines the interface for "physical graphs" --- on-disk storage hierarchies.
 *
 * @author malensek
 */
public interface PhysicalGraph {

    /**
     * Retrieves a {@link Block} instance, given its path on disk.
     *
     * @param blockPath the physical (on-disk) location of the Block to load.
     *
     * @return Block stored at blockPath.
     */
    public Block loadBlock(String blockPath)
        throws IOException, SerializationException;

    /**
     * Retrieves a {@link Metadata} instance, given a {@link Block} path on
     * disk.
     *
     * @param blockPath the physical location of the Block to load metadata
     * from.
     *
     * @return Metadata stored in the Block specified by blockPath.
     */
    public Metadata loadMetadata(String blockPath)
        throws IOException, SerializationException;

    /**
     * Stores a {@link Block} on disk.  The location of the Block will be
     * determined by the particular FileSystem implementation being used rather
     * than as a method parameter, so only the Block itself is provided.
     *
     * @param block the Block instance to persist to disk.
     *
     * @return String representation of the Block path on disk.
     */
    public String storeBlock(Block block)
        throws FileSystemException, IOException;

    /**
     * Inserts Metadata into the file system.  In many cases, Metadata is not
     * stored individually on disk but placed in an index instead.  This method
     * is useful during a full recovery operation for re-linking indexed
     * Metadata with its associated files on disk, or could be used in
     * situations where information should only be indexed and not stored.
     *
     * @param metadata the {@link Metadata} to 'store,' which may just involve
     * updating index structures.
     * @param blockPath the on-disk path of the Block the Metadata being stored
     * belongs to.
     */
    public void storeMetadata(Metadata metadata, String blockPath)
        throws FileSystemException, IOException;
}
