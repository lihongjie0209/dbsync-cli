package io.dbsync.config;

import jakarta.enterprise.context.ApplicationScoped;
import org.yaml.snakeyaml.LoaderOptions;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.Constructor;

import java.io.FileInputStream;
import java.io.InputStream;
import java.util.Map;

@ApplicationScoped
public class ConfigLoader {

    public SyncConfig load(String configFilePath, Map<String, String> overrides) throws Exception {
        Yaml yaml = new Yaml(new Constructor(SyncConfig.class, new LoaderOptions()));
        SyncConfig config;

        try (InputStream is = new FileInputStream(configFilePath)) {
            config = yaml.load(is);
        }

        if (config == null) {
            config = new SyncConfig();
        }
        if (config.getSource() == null) config.setSource(new DatabaseConfig());
        if (config.getTarget() == null) config.setTarget(new DatabaseConfig());
        if (config.getSync() == null) config.setSync(new SyncOptions());

        applyOverrides(config, overrides);
        config.validate();
        return config;
    }

    private void applyOverrides(SyncConfig config, Map<String, String> overrides) {
        overrides.forEach((key, value) -> {
            switch (key) {
                case "source.type"     -> config.getSource().setType(value);
                case "source.host"     -> config.getSource().setHost(value);
                case "source.port"     -> config.getSource().setPort(Integer.parseInt(value));
                case "source.database" -> config.getSource().setDatabase(value);
                case "source.username" -> config.getSource().setUsername(value);
                case "source.password" -> config.getSource().setPassword(value);
                case "target.type"     -> config.getTarget().setType(value);
                case "target.host"     -> config.getTarget().setHost(value);
                case "target.port"     -> config.getTarget().setPort(Integer.parseInt(value));
                case "target.database" -> config.getTarget().setDatabase(value);
                case "target.username" -> config.getTarget().setUsername(value);
                case "target.password" -> config.getTarget().setPassword(value);
            }
        });
    }
}
