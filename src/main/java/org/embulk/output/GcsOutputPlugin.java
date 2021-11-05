package org.embulk.output;

import com.google.cloud.storage.Storage;
import com.google.common.annotations.VisibleForTesting;
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

import java.io.IOException;
import java.security.GeneralSecurityException;
import java.util.List;
import java.util.Optional;

public class GcsOutputPlugin implements FileOutputPlugin
{
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
}
