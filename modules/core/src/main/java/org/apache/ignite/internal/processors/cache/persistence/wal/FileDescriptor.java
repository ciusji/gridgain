/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.cache.persistence.wal;

import java.io.File;
import java.io.IOException;
import java.util.Enumeration;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;
import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.processors.cache.persistence.file.FileIO;
import org.apache.ignite.internal.processors.cache.persistence.file.FileIOFactory;
import org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager;
import org.apache.ignite.internal.processors.cache.persistence.file.UnzipFileIO;
import org.apache.ignite.internal.processors.cache.persistence.wal.io.SegmentIO;
import org.apache.ignite.internal.util.typedef.internal.SB;
import org.jetbrains.annotations.Nullable;

import static java.nio.file.StandardOpenOption.READ;

/**
 * WAL file descriptor.
 */
public class FileDescriptor implements Comparable<FileDescriptor>, AbstractWalRecordsIterator.AbstractFileDescriptor {
    /** file extension of WAL segment. */
    public static final String WAL_SEGMENT_FILE_EXT = ".wal";

    /** Length of WAL segment file name. */
    private static final int WAL_SEGMENT_FILE_NAME_LENGTH = 16;

    /** File represented by this class. */
    protected final File file;

    /** Absolute WAL segment file index. */
    protected final long idx;

    /**
     * Creates file descriptor. Index is restored from file name.
     *
     * @param file WAL segment file.
     */
    public FileDescriptor(File file) {
        this(file, null);
    }

    /**
     *  Creates file descriptor.
     *
     * @param file WAL segment file.
     * @param idx Absolute WAL segment file index. For null value index is restored from file name.
     */
    public FileDescriptor(File file, @Nullable Long idx) {
        this.file = file;

        String fileName = file.getName();

        assert fileName.contains(WAL_SEGMENT_FILE_EXT);

        this.idx = idx == null ? Long.parseLong(fileName.substring(0, WAL_SEGMENT_FILE_NAME_LENGTH)) : idx;
    }

    /**
     * Getting segment file name.
     *
     * @param idx Segment index.
     * @return Segment file name.
     */
    public static String fileName(long idx) {
        SB b = new SB();

        String segmentStr = Long.toString(idx);

        for (int i = segmentStr.length(); i < WAL_SEGMENT_FILE_NAME_LENGTH; i++)
            b.a('0');

        b.a(segmentStr).a(WAL_SEGMENT_FILE_EXT);

        return b.toString();
    }

    /** {@inheritDoc} */
    @Override public int compareTo(FileDescriptor o) {
        return Long.compare(idx, o.idx);
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object o) {
        if (this == o)
            return true;

        if (!(o instanceof FileDescriptor))
            return false;

        FileDescriptor that = (FileDescriptor)o;

        return idx == that.idx;
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        return (int)(idx ^ (idx >>> 32));
    }

    /**
     * Return absolute WAL segment file index.
     *
     * @return Absolute WAL segment file index.
     */
    public long getIdx() {
        return idx;
    }

    /**
     * Return absolute pathname string of this file descriptor pathname.
     *
     * @return Absolute pathname string of this file descriptor pathname.
     */
    public String getAbsolutePath() {
        return file.getAbsolutePath();
    }

    /** {@inheritDoc} */
    @Override public boolean isCompressed() {
        return file.getName().endsWith(FilePageStoreManager.ZIP_SUFFIX);
    }

    /** {@inheritDoc} */
    @Override public File file() {
        return file;
    }

    /** {@inheritDoc} */
    @Override public long idx() {
        return idx;
    }

    /** {@inheritDoc} */
    @Override public SegmentIO toReadOnlyIO(FileIOFactory fileIOFactory) throws IOException {
        FileIO fileIO = isCompressed() ? new UnzipFileIO(file()) : fileIOFactory.create(file(), READ);

        return new SegmentIO(idx, fileIO);
    }

    /**
     * Calculate full size of segment in bytes, if it is compressed then size of compressed data will be added.
     *
     * @return Full segment size in bytes.
     */
    public long fullSize() {
        long size = file.length();

        if (isCompressed()) {
            try (ZipFile zipFile = new ZipFile(file)) {
                Enumeration<? extends ZipEntry> entries = zipFile.entries();

                if (!entries.hasMoreElements())
                    throw new IOException("Failed to read entry from compressed file: " + file);

                size += entries.nextElement().getSize();
            }
            catch (IOException e) {
                throw new IgniteException(e);
            }
        }

        return size;
    }
}
