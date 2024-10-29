/*
 * Copyright 2015 Kazuyuki Honda, and the Embulk project
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.embulk.output.gcs;

import com.google.api.gax.paging.Page;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.Storage;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Throwables;
import org.embulk.config.ConfigDiff;
import org.embulk.config.ConfigException;
import org.embulk.config.ConfigSource;
import org.embulk.config.TaskReport;
import org.embulk.config.TaskSource;
import org.embulk.spi.FileOutputPlugin;
import org.embulk.spi.TransactionalFileOutput;
import org.embulk.util.config.ConfigMapper;
import org.embulk.util.config.ConfigMapperFactory;
import org.embulk.util.config.TaskMapper;
import org.embulk.util.config.units.LocalFile;
import org.embulk.util.retryhelper.RetryExecutor;
import org.embulk.util.retryhelper.RetryGiveupException;
import org.embulk.util.retryhelper.Retryable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.lang.invoke.MethodHandles;
import java.security.GeneralSecurityException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

public class GcsOutputPlugin implements FileOutputPlugin
{
    private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
    public static final ConfigMapperFactory CONFIG_MAPPER_FACTORY = ConfigMapperFactory.builder()
            .addDefaultModules().build();
    public static final ConfigMapper CONFIG_MAPPER = CONFIG_MAPPER_FACTORY.createConfigMapper();
    public static final TaskMapper TASK_MAPPER = CONFIG_MAPPER_FACTORY.createTaskMapper();
    @Override
    public ConfigDiff transaction(ConfigSource config,
                                  int taskCount,
                                  FileOutputPlugin.Control control)
    {
        PluginTask task = CONFIG_MAPPER.map(config, PluginTask.class);

        if (task.getP12KeyfilePath().isPresent()) {
            if (task.getP12Keyfile().isPresent()) {
                throw new ConfigException("Setting both p12_keyfile_path and p12_keyfile is invalid");
            }
            try {
                task.setP12Keyfile(Optional.of(LocalFile.of(task.getP12KeyfilePath().get())));
            }
            catch (IOException ex) {
                throw new RuntimeException(ex);
            }
        }

        if (task.getAuthMethod().getString().equals("json_key")) {
            if (!task.getJsonKeyfile().isPresent()) {
                throw new ConfigException("If auth_method is json_key, you have to set json_keyfile");
            }
        }
        else if (task.getAuthMethod().getString().equals("private_key")) {
            if (!task.getP12Keyfile().isPresent() || !task.getServiceAccountEmail().isPresent()) {
                throw new ConfigException("If auth_method is private_key, you have to set both service_account_email and p12_keyfile");
            }
        }

        if (task.getDeleteInAdvance()) {
            deleteFiles(task);
        }

        return resume(task.toTaskSource(), taskCount, control);
    }

    @Override
    public ConfigDiff resume(TaskSource taskSource,
                             int taskCount,
                             FileOutputPlugin.Control control)
    {
        control.run(taskSource);
        return CONFIG_MAPPER_FACTORY.newConfigDiff();
    }

    @Override
    public void cleanup(TaskSource taskSource,
                        int taskCount,
                        List<TaskReport> successTaskReports)
    {
    }

    @Override
    public TransactionalFileOutput open(TaskSource taskSource, final int taskIndex)
    {
        PluginTask task = TASK_MAPPER.map(taskSource, PluginTask.class);

        Storage client = createClient(task);
        return new GcsTransactionalFileOutput(task, client, taskIndex);
    }

    private GcsAuthentication newGcsAuth(PluginTask task)
    {
        try {
            return new GcsAuthentication(task);
        }
        catch (GeneralSecurityException | IOException ex) {
            throw new ConfigException(ex);
        }
    }

    @VisibleForTesting
    public Storage createClient(final PluginTask task)
    {
        try {
            GcsAuthentication auth = newGcsAuth(task);
            return auth.getGcsClient();
        }
        catch (ConfigException | IOException ex) {
            throw new RuntimeException(ex);
        }
    }

    public void deleteFiles(PluginTask task)
    {
        logger.info("Start delete files operation");
        Storage client = createClient(task);
        try {
            List<BlobId> blobIds = listObjectsWithRetry(client, task.getBucket(), task.getPathPrefix(), task.getMaxConnectionRetry());
            if (blobIds.isEmpty()) {
                logger.info("no files were found");
                return;
            }
            for (BlobId blobId : blobIds) {
                deleteObjectWithRetry(client, blobId, task.getMaxConnectionRetry());
                logger.info("delete file: {}/{}", blobId.getBucket(), blobId.getName());
            }
        }
        catch (IOException ex) {
            throw new ConfigException(ex);
        }
    }

    private List<BlobId> listObjectsWithRetry(Storage client, String bucket, String prefix, int maxConnectionRetry) throws IOException
    {
        try {
            return RetryExecutor.builder()
                    .withRetryLimit(maxConnectionRetry)
                    .withInitialRetryWaitMillis(500)
                    .withMaxRetryWaitMillis(30 * 1000)
                    .build()
                    .runInterruptible(new Retryable<List<BlobId>>() {
                        @Override
                        public List<BlobId> call() throws IOException
                        {
                            // https://cloud.google.com/storage/docs/samples/storage-list-files-with-prefix#storage_list_files_with_prefix-java
                            Page<Blob> list = client.list(bucket, Storage.BlobListOption.prefix(prefix), Storage.BlobListOption.currentDirectory());
                            List<BlobId> blobIds = new ArrayList<>();
                            list.iterateAll().forEach(x -> blobIds.add(x.getBlobId()));
                            return blobIds;
                        }

                        @Override
                        public boolean isRetryableException(Exception exception)
                        {
                            return true;
                        }

                        @Override
                        public void onRetry(Exception exception, int retryCount, int retryLimit, int retryWait) throws RetryGiveupException
                        {
                            String message = String.format("GCS list request failed. Retrying %d/%d after %d seconds. Message: %s: %s",
                                    retryCount, retryLimit, retryWait / 1000, exception.getClass(), exception.getMessage());
                            if (retryCount % 3 == 0) {
                                logger.warn(message, exception);
                            }
                            else {
                                logger.warn(message);
                            }
                        }

                        @Override
                        public void onGiveup(Exception firstException, Exception lastException) throws RetryGiveupException
                        {
                        }
                    });
        }
        catch (RetryGiveupException ex) {
            throw Throwables.propagate(ex.getCause());
        }
        catch (InterruptedException ex) {
            throw new InterruptedIOException();
        }
    }

    private Void deleteObjectWithRetry(Storage client, BlobId blobId, int maxConnectionRetry) throws IOException
    {
        try {
            return RetryExecutor.builder()
                    .withRetryLimit(maxConnectionRetry)
                    .withInitialRetryWaitMillis(500)
                    .withMaxRetryWaitMillis(30 * 1000)
                    .build()
                    .runInterruptible(new Retryable<Void>() {
                        @Override
                        public Void call() throws IOException
                        {
                            client.delete(blobId);
                            return null;
                        }

                        @Override
                        public boolean isRetryableException(Exception exception)
                        {
                            return true;
                        }

                        @Override
                        public void onRetry(Exception exception, int retryCount, int retryLimit, int retryWait) throws RetryGiveupException
                        {
                            String message = String.format("GCS delete request failed. Retrying %d/%d after %d seconds. Message: %s: %s",
                                    retryCount, retryLimit, retryWait / 1000, exception.getClass(), exception.getMessage());
                            if (retryCount % 3 == 0) {
                                logger.warn(message, exception);
                            }
                            else {
                                logger.warn(message);
                            }
                        }

                        @Override
                        public void onGiveup(Exception firstException, Exception lastException) throws RetryGiveupException
                        {
                        }
                    });
        }
        catch (RetryGiveupException ex) {
            throw Throwables.propagate(ex.getCause());
        }
        catch (InterruptedException ex) {
            throw new InterruptedIOException();
        }
    }
}
