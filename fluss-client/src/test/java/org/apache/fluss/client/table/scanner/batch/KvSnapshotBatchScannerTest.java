/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.fluss.client.table.scanner.batch;

import org.apache.fluss.client.table.scanner.RemoteFileDownloader;
import org.apache.fluss.exception.FlussRuntimeException;
import org.apache.fluss.fs.FSDataInputStream;
import org.apache.fluss.fs.FileSystem;
import org.apache.fluss.fs.FsPath;
import org.apache.fluss.fs.FsPathAndFileName;
import org.apache.fluss.fs.local.LocalFileSystem;
import org.apache.fluss.metadata.KvFormat;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.record.TestingSchemaGetter;
import org.apache.fluss.row.encode.KvValueLayout;
import org.apache.fluss.types.DataTypes;
import org.apache.fluss.utils.CloseableRegistry;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.apache.fluss.testutils.common.CommonTestUtils.waitUntil;
import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link KvSnapshotBatchScanner}. */
class KvSnapshotBatchScannerTest {

    private static final Schema SCHEMA =
            Schema.newBuilder()
                    .primaryKey("id")
                    .column("id", DataTypes.INT())
                    .column("name", DataTypes.STRING())
                    .build();

    @Test
    void testClosingScannerStopsDownloadAndCleansUpDirectory(@TempDir Path tempDir)
            throws Exception {
        CountDownLatch downloadStarted = new CountDownLatch(1);
        CountDownLatch downloadStopped = new CountDownLatch(1);
        AtomicBoolean downloadInterrupted = new AtomicBoolean(false);

        RemoteFileDownloader blockingDownloader =
                new RemoteFileDownloader(1) {
                    @Override
                    public void transferAllToDirectory(
                            List<FsPathAndFileName> fsPathAndFileNames,
                            Path targetDirectory,
                            CloseableRegistry closeableRegistry)
                            throws IOException {
                        downloadStarted.countDown();
                        try {
                            new CountDownLatch(1).await();
                        } catch (InterruptedException e) {
                            downloadInterrupted.set(true);
                            Thread.currentThread().interrupt();
                            throw new IOException("Download interrupted", e);
                        } finally {
                            downloadStopped.countDown();
                        }

                        if (closeableRegistry.isClosed()) {
                            downloadInterrupted.set(true);
                            throw new IOException("Stream or registry closed");
                        }
                    }
                };

        TableBucket tableBucket = new TableBucket(1L, 0);
        FsPath dummyPath = new FsPath(tempDir.resolve("dummy-remote-file").toUri());
        List<FsPathAndFileName> files =
                Collections.singletonList(new FsPathAndFileName(dummyPath, "file.sst"));

        TestingSchemaGetter schemaGetter = new TestingSchemaGetter(1, SCHEMA);

        KvSnapshotBatchScanner scanner =
                new KvSnapshotBatchScanner(
                        1,
                        SCHEMA,
                        schemaGetter,
                        tableBucket,
                        files,
                        null,
                        tempDir.toString(),
                        KvValueLayout.PLAIN,
                        KvFormat.COMPACTED,
                        blockingDownloader);

        try {
            assertThat(downloadStarted.await(10, TimeUnit.SECONDS)).isTrue();
            // Verify pollBatch returns NO_DATA_AVAILABLE while download is blocked
            assertThat(scanner.pollBatch(Duration.ofMillis(50)))
                    .isSameAs(KvSnapshotBatchScanner.NO_DATA_AVAILABLE);
        } finally {
            scanner.close();
            assertThat(downloadStopped.await(10, TimeUnit.SECONDS)).isTrue();
            blockingDownloader.close();
        }

        assertThat(downloadInterrupted.get()).isTrue();

        // Local snapshot directories matching kv-snapshots-* should be cleaned up
        File[] localDirs =
                tempDir.toFile()
                        .listFiles(
                                (dir, name) ->
                                        name.startsWith("kv-snapshots-")
                                                && new File(dir, name).isDirectory());
        assertThat(localDirs).isEmpty();
    }

    @Test
    void testClosingScannerDefersCleanupUntilDownloadWorkerStops(@TempDir Path tempDir)
            throws Exception {
        CountDownLatch openStarted = new CountDownLatch(1);
        CountDownLatch releaseOpen = new CountDownLatch(1);

        FileSystem fileSystem =
                new LocalFileSystem() {
                    @Override
                    public FSDataInputStream open(FsPath path) {
                        openStarted.countDown();
                        boolean interrupted = false;
                        while (true) {
                            try {
                                releaseOpen.await();
                                break;
                            } catch (InterruptedException e) {
                                interrupted = true;
                            }
                        }
                        if (interrupted) {
                            Thread.currentThread().interrupt();
                        }
                        return new FSDataInputStream() {
                            @Override
                            public int read() {
                                return -1;
                            }

                            @Override
                            public void close() {}

                            @Override
                            public void seek(long desired) {}

                            @Override
                            public long getPos() {
                                return 0;
                            }
                        };
                    }
                };

        FsPath remotePath =
                new FsPath(tempDir.resolve("dummy-remote-file").toUri()) {
                    @Override
                    public FileSystem getFileSystem() {
                        return fileSystem;
                    }
                };
        List<FsPathAndFileName> files =
                Collections.singletonList(new FsPathAndFileName(remotePath, "file.sst"));
        RemoteFileDownloader downloader = new RemoteFileDownloader(1);
        KvSnapshotBatchScanner scanner =
                new KvSnapshotBatchScanner(
                        1,
                        SCHEMA,
                        new TestingSchemaGetter(1, SCHEMA),
                        new TableBucket(1L, 0),
                        files,
                        null,
                        tempDir.toString(),
                        KvValueLayout.PLAIN,
                        KvFormat.COMPACTED,
                        downloader);

        Thread closeThread = new Thread(scanner::close);
        try {
            assertThat(openStarted.await(10, TimeUnit.SECONDS)).isTrue();
            closeThread.start();
            waitUntil(
                    () -> closeThread.getState() == Thread.State.TIMED_WAITING,
                    Duration.ofSeconds(10),
                    "Scanner close did not enter its bounded wait");
            assertThat(closeThread.isAlive()).isTrue();
            closeThread.join(TimeUnit.SECONDS.toMillis(10));
            assertThat(closeThread.isAlive()).isFalse();
            assertThat(
                            tempDir.toFile()
                                    .listFiles(
                                            (dir, name) ->
                                                    name.startsWith("kv-snapshots-")
                                                            && new File(dir, name).isDirectory()))
                    .hasSize(1);
            releaseOpen.countDown();
            waitUntil(
                    () ->
                            tempDir.toFile()
                                            .listFiles(
                                                    (dir, name) ->
                                                            name.startsWith("kv-snapshots-")
                                                                    && new File(dir, name)
                                                                            .isDirectory())
                                            .length
                                    == 0,
                    Duration.ofSeconds(10),
                    "Snapshot directory was not removed after download worker stopped");
        } finally {
            releaseOpen.countDown();
            scanner.close();
            downloader.close();
        }

        assertThat(
                        tempDir.toFile()
                                .listFiles(
                                        (dir, name) ->
                                                name.startsWith("kv-snapshots-")
                                                        && new File(dir, name).isDirectory()))
                .isEmpty();
    }

    @Test
    void testPollBatchReportsDownloadFailure(@TempDir Path tempDir) throws Exception {
        CountDownLatch downloadStarted = new CountDownLatch(1);
        CountDownLatch failDownload = new CountDownLatch(1);
        RemoteFileDownloader failingDownloader =
                new RemoteFileDownloader(1) {
                    @Override
                    public void transferAllToDirectory(
                            List<FsPathAndFileName> fsPathAndFileNames,
                            Path targetDirectory,
                            CloseableRegistry closeableRegistry)
                            throws IOException {
                        downloadStarted.countDown();
                        try {
                            failDownload.await();
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                            throw new IOException("Download interrupted", e);
                        }
                        throw new IOException("Injected download failure");
                    }
                };
        FsPath remotePath = new FsPath(tempDir.resolve("dummy-remote-file").toUri());
        List<FsPathAndFileName> files =
                Collections.singletonList(new FsPathAndFileName(remotePath, "file.sst"));
        KvSnapshotBatchScanner scanner =
                new KvSnapshotBatchScanner(
                        1,
                        SCHEMA,
                        new TestingSchemaGetter(1, SCHEMA),
                        new TableBucket(1L, 0),
                        files,
                        null,
                        tempDir.toString(),
                        KvValueLayout.PLAIN,
                        KvFormat.COMPACTED,
                        failingDownloader);
        CountDownLatch pollStarted = new CountDownLatch(1);
        AtomicReference<Exception> pollFailure = new AtomicReference<>();
        Thread pollThread =
                new Thread(
                        () -> {
                            pollStarted.countDown();
                            try {
                                scanner.pollBatch(Duration.ofSeconds(10));
                            } catch (Exception e) {
                                pollFailure.set(e);
                            }
                        });

        try {
            assertThat(downloadStarted.await(10, TimeUnit.SECONDS)).isTrue();
            pollThread.start();
            assertThat(pollStarted.await(10, TimeUnit.SECONDS)).isTrue();
            waitUntil(
                    () -> pollThread.getState() == Thread.State.TIMED_WAITING,
                    Duration.ofSeconds(10),
                    "pollBatch did not wait for snapshot reader initialization");
            failDownload.countDown();
            pollThread.join(TimeUnit.SECONDS.toMillis(10));
            assertThat(pollThread.isAlive()).isFalse();
            assertThat(pollFailure.get())
                    .isInstanceOf(FlussRuntimeException.class)
                    .hasMessageContaining("Failed to initialize snapshot files reader.")
                    .hasCauseInstanceOf(IOException.class);
        } finally {
            failDownload.countDown();
            scanner.close();
            failingDownloader.close();
        }
    }
}
