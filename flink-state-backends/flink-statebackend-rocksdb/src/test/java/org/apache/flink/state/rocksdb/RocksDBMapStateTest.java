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

import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.util.IOUtils;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.FlushOptions;
import org.rocksdb.MutableColumnFamilyOptions;
import org.rocksdb.TableProperties;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests prefix isolation of RocksDB map state operations. */
class RocksDBMapStateTest {

    private static final int NEIGHBOR_TOMBSTONES = 1024;
    private static final int MAP_ENTRIES = 257;
    private static final MapStateDescriptor<Integer, Integer> STATE_DESCRIPTOR =
            new MapStateDescriptor<>("map", IntSerializer.INSTANCE, IntSerializer.INSTANCE);

    @TempDir private Path temporaryDirectory;

    private RocksDBKeyedStateBackend<Integer> backend;
    private ColumnFamilyHandle columnFamily;

    @BeforeEach
    void setUp() throws Exception {
        backend =
                RocksDBTestUtils.builderForTestDefaults(
                                temporaryDirectory.toFile(),
                                IntSerializer.INSTANCE,
                                1,
                                new KeyGroupRange(0, 0),
                                Collections.emptyList())
                        .build();
        mapState(0, 0);
        columnFamily = backend.getColumnFamilyHandle(STATE_DESCRIPTOR.getName());
        backend.db.setOptions(
                columnFamily,
                MutableColumnFamilyOptions.builder().setDisableAutoCompactions(true).build());
        // Allow the entire map to be deleted, but fail if a seek scans the neighbor's tombstones.
        backend.getReadOptions().setMaxSkippableInternalKeys(2L * MAP_ENTRIES + 1);
    }

    @AfterEach
    void tearDown() throws Exception {
        if (backend != null) {
            IOUtils.closeQuietly(backend);
            backend.dispose();
        }
    }

    @ParameterizedTest
    @EnumSource(EmptyMapOperation.class)
    void testEmptyMapDoesNotScanDeletedEntriesOfNextKey(EmptyMapOperation operation)
            throws Exception {
        createNeighborTombstones();
        final MapState<Integer, Integer> state = mapState(0, 0);

        switch (operation) {
            case ENTRIES:
                assertThat(state.entries()).isEmpty();
                break;
            case IS_EMPTY:
                assertThat(state.isEmpty()).isTrue();
                break;
            case CLEAR:
                state.clear();
                break;
            default:
                throw new IllegalArgumentException("Unknown map operation: " + operation);
        }

        assertNeighborPreserved();
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testIteratorCacheReloadDoesNotScanDeletedEntriesOfNextKey(boolean removeEntries)
            throws Exception {
        createNeighborTombstones();
        final MapState<Integer, Integer> state = mapState(0, 0);
        populateMap(state);

        final List<Integer> keys = new ArrayList<>();
        final Iterator<Map.Entry<Integer, Integer>> iterator = state.iterator();
        while (iterator.hasNext()) {
            final Map.Entry<Integer, Integer> entry = iterator.next();
            keys.add(entry.getKey());
            assertThat(entry.getValue()).isEqualTo(entry.getKey());
            if (removeEntries) {
                iterator.remove();
            }
        }

        assertThat(keys)
                .containsExactlyElementsOf(
                        IntStream.range(0, MAP_ENTRIES).boxed().collect(Collectors.toList()));
        assertThat(state.isEmpty()).isEqualTo(removeEntries);
        assertNeighborPreserved();
    }

    @Test
    void testClearDoesNotScanDeletedEntriesOfNextKey() throws Exception {
        createNeighborTombstones();
        final MapState<Integer, Integer> state = mapState(0, 0);
        populateMap(state);

        state.clear();

        assertThat(state.isEmpty()).isTrue();
        assertNeighborPreserved();
    }

    @ParameterizedTest
    @ValueSource(ints = {127, 128})
    void testIteratorCacheReloadAfterEntriesWereRemoved(int firstRemovedKey) throws Exception {
        createNeighborTombstones();
        final MapState<Integer, Integer> state = mapState(0, 0);
        populateMap(state);
        final Iterator<Map.Entry<Integer, Integer>> iterator = state.iterator();
        for (int i = 0; i < 128; i++) {
            assertThat(iterator.next().getKey()).isEqualTo(i);
        }

        // Removing from key 127 also removes the entry the iterator returned last, so the next
        // cache refill seeks to a deleted key and its bounded seek is exhausted at once. Removing
        // from key 128 keeps that entry, so the refill finds it and has to skip it first.
        for (int i = firstRemovedKey; i < MAP_ENTRIES; i++) {
            state.remove(i);
        }

        assertThat(iterator.hasNext()).isFalse();
        assertNeighborPreserved();
    }

    private void createNeighborTombstones() throws Exception {
        final MapState<Integer, Integer> neighbor = mapState(1, 0);
        for (int i = 0; i < NEIGHBOR_TOMBSTONES; i++) {
            neighbor.put(i, i);
        }
        flush();
        for (int i = 0; i < NEIGHBOR_TOMBSTONES; i++) {
            neighbor.remove(i);
        }
        flush();

        assertThat(
                        backend.db.getPropertiesOfAllTables(columnFamily).values().stream()
                                .mapToLong(TableProperties::getNumDeletions)
                                .sum())
                .isEqualTo(NEIGHBOR_TOMBSTONES);
        mapState(2, 0).put(NEIGHBOR_TOMBSTONES, NEIGHBOR_TOMBSTONES);
    }

    private void populateMap(MapState<Integer, Integer> state) throws Exception {
        for (int i = 0; i < MAP_ENTRIES; i++) {
            state.put(i, i);
        }
    }

    private void assertNeighborPreserved() throws Exception {
        assertThat(mapState(2, 0).get(NEIGHBOR_TOMBSTONES)).isEqualTo(NEIGHBOR_TOMBSTONES);
    }

    private MapState<Integer, Integer> mapState(int key, int namespace) throws Exception {
        backend.setCurrentKey(key);
        return backend.getPartitionedState(namespace, IntSerializer.INSTANCE, STATE_DESCRIPTOR);
    }

    private void flush() throws Exception {
        try (FlushOptions options = new FlushOptions().setWaitForFlush(true)) {
            backend.db.flush(options, columnFamily);
        }
    }

    private enum EmptyMapOperation {
        ENTRIES,
        IS_EMPTY,
        CLEAR
    }
}
