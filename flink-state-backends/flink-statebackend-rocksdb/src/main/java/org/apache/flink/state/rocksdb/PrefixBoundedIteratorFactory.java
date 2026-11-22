/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.state.rocksdb;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.IOUtils;
import org.apache.flink.util.Preconditions;

import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.DirectSlice;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksIterator;
import org.rocksdb.Slice;

import javax.annotation.Nullable;

import java.nio.ByteBuffer;

/**
 * Creates prefix-bounded iterators that can reuse a native upper-bound slice.
 *
 * <p>The factory copies the read options when it is created. Its bounded iterators do not see later
 * changes to them.
 *
 * <p>RocksDB reads the bound through a pointer for as long as the iterator is open. Overlapping
 * iterators and bounds that exceed the buffer capacity therefore get a slice of their own.
 *
 * <p>The factory must be closed after all of its iterators.
 */
final class PrefixBoundedIteratorFactory implements AutoCloseable {

    /** The longest bound that fits into the reused buffer. */
    @VisibleForTesting static final int MAX_REUSED_BOUND_LENGTH = 256;

    private final ReadOptions readOptions;

    /** The copy for bounded iterators. It has no upper bound and is never modified. */
    private final ReadOptions boundedReadOptions;

    @Nullable private ByteBuffer reusedBound;
    @Nullable private DirectSlice reusedBoundSlice;

    /** A copy of {@link #boundedReadOptions} whose upper bound is {@link #reusedBoundSlice}. */
    @Nullable private ReadOptions reusedBoundReadOptions;

    private boolean reusedBoundInUse;
    private boolean directMemoryUnavailable;
    private boolean closed;

    /**
     * @param readOptions the options to copy. They are not modified, and they are used as they are
     *     for a prefix that has no exclusive upper bound.
     */
    PrefixBoundedIteratorFactory(ReadOptions readOptions) {
        this.readOptions = readOptions;
        this.boundedReadOptions =
                RocksDBOperationUtils.copyForPrefixBoundedIterators(readOptions, null);
    }

    /**
     * Creates an iterator that stops at the end of the key range starting with {@code prefix}. The
     * prefix only sets the bound; callers still position the iterator with {@code seek}.
     */
    RocksIteratorWrapper newIterator(
            RocksDB db, ColumnFamilyHandle columnFamilyHandle, byte[] prefix) {
        final byte[] prefixEnd = RocksDBOperationUtils.getPrefixEnd(prefix);
        if (prefixEnd == null) {
            return RocksDBOperationUtils.getRocksIterator(db, columnFamilyHandle, readOptions);
        }

        final ReadOptions reusedBoundOptions = tryAcquireBound(prefixEnd);
        if (reusedBoundOptions == null) {
            return newIteratorWithOwnBound(db, columnFamilyHandle, prefixEnd);
        }

        try {
            return new ReusedBoundIterator(db.newIterator(columnFamilyHandle, reusedBoundOptions));
        } catch (RuntimeException | Error e) {
            releaseReusedBound();
            throw e;
        }
    }

    private RocksIteratorWrapper newIteratorWithOwnBound(
            RocksDB db, ColumnFamilyHandle columnFamilyHandle, byte[] prefixEnd) {
        final Slice upperBound = new Slice(prefixEnd);
        ReadOptions ownReadOptions = null;
        try {
            ownReadOptions = new ReadOptions(boundedReadOptions);
            ownReadOptions.setIterateUpperBound(upperBound);
            return new RocksIteratorWrapper(
                    db.newIterator(columnFamilyHandle, ownReadOptions), ownReadOptions, upperBound);
        } catch (RuntimeException | Error e) {
            IOUtils.closeQuietly(ownReadOptions);
            IOUtils.closeQuietly(upperBound);
            throw e;
        }
    }

    /**
     * Writes the bound into the reused buffer if no open iterator uses it.
     *
     * @return the read options that point to the reused bound, or {@code null} if the iterator
     *     needs a bound of its own
     */
    @Nullable
    private synchronized ReadOptions tryAcquireBound(byte[] prefixEnd) {
        Preconditions.checkState(!closed, "The iterator factory is closed.");
        if (reusedBoundInUse
                || prefixEnd.length > MAX_REUSED_BOUND_LENGTH
                || directMemoryUnavailable) {
            return null;
        }
        if (reusedBound == null) {
            final ByteBuffer buffer;
            try {
                buffer = ByteBuffer.allocateDirect(MAX_REUSED_BOUND_LENGTH);
            } catch (OutOfMemoryError e) {
                if (!ExceptionUtils.isDirectOutOfMemoryError(e)) {
                    throw e;
                }
                // The private RocksDB slice does not use the JVM's direct-buffer allowance.
                directMemoryUnavailable = true;
                return null;
            }
            buffer.limit(prefixEnd.length);
            final DirectSlice slice = new DirectSlice(buffer, prefixEnd.length);
            try {
                reusedBoundReadOptions =
                        new ReadOptions(boundedReadOptions).setIterateUpperBound(slice);
            } catch (RuntimeException | Error e) {
                IOUtils.closeQuietly(slice);
                throw e;
            }
            reusedBoundSlice = slice;
            reusedBound = buffer;
        }
        if (prefixEnd.length != reusedBound.limit()) {
            reusedBoundSlice.setLength(prefixEnd.length);
            reusedBound.limit(prefixEnd.length);
        }
        reusedBound.rewind();
        reusedBound.put(prefixEnd);
        reusedBoundInUse = true;
        return reusedBoundReadOptions;
    }

    private synchronized void releaseReusedBound() {
        reusedBoundInUse = false;
    }

    @VisibleForTesting
    synchronized boolean isReusedBoundInUse() {
        return reusedBoundInUse;
    }

    @Override
    public synchronized void close() {
        closed = true;
        IOUtils.closeQuietly(reusedBoundReadOptions);
        IOUtils.closeQuietly(reusedBoundSlice);
        IOUtils.closeQuietly(boundedReadOptions);
        reusedBoundReadOptions = null;
        reusedBoundSlice = null;
        reusedBound = null;
    }

    /** An iterator whose bound is in the reused buffer, which it releases when it is closed. */
    private final class ReusedBoundIterator extends RocksIteratorWrapper {

        private boolean released;

        private ReusedBoundIterator(RocksIterator iterator) {
            super(iterator);
        }

        @Override
        public void close() {
            // The buffer may only get another bound after the native iterator is closed.
            super.close();
            // A second close must not release the buffer for an iterator that got it since.
            if (!released) {
                released = true;
                releaseReusedBound();
            }
        }
    }
}
