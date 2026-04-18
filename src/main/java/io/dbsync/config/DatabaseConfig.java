package io.dbsync.config;

public class DatabaseConfig {

    private String type;      // mysql | postgresql
    private String host = "localhost";
    private int port;
    private String database;
    private String username;
    private String password;

    // MySQL-specific
    private int serverId = 12345;

    // PostgreSQL-specific
    private String slotName = "dbsync_slot";
    private String pluginName; // null → use type-appropriate default

    private String url; // optional JDBC URL override (useful for testing)

    public String getUrl() { return url; }
    public void setUrl(String url) { this.url = url; }

    public String getType() { return type; }
    public void setType(String type) { this.type = type; }

    public String getHost() { return host; }
    public void setHost(String host) { this.host = host; }

    public int getPort() {
        if (port > 0) return port;
        return switch (type) {
            case "postgresql" -> 5432;
            case "kingbase"   -> 54321;
            default           -> 3306;
        };
    }
    public void setPort(int port) { this.port = port; }

    public String getDatabase() { return database; }
    public void setDatabase(String database) { this.database = database; }

    public String getUsername() { return username; }
    public void setUsername(String username) { this.username = username; }

    public String getPassword() { return password; }
    public void setPassword(String password) { this.password = password; }

    public int getServerId() { return serverId; }
    public void setServerId(int serverId) { this.serverId = serverId; }

    public String getSlotName() { return slotName; }
    public void setSlotName(String slotName) { this.slotName = slotName; }

    public String getPluginName() {
        if (pluginName != null && !pluginName.isEmpty()) return pluginName;
        // pgoutput is the default for plain PostgreSQL;
        // KingbaseES ships decoderbufs but NOT pgoutput.
        return "kingbase".equals(type) ? "decoderbufs" : "pgoutput";
    }
    public void setPluginName(String pluginName) { this.pluginName = pluginName; }

    public String jdbcUrl() {
        if (url != null && !url.isEmpty()) return url;
        return switch (type) {
            case "mysql" -> String.format(
                "jdbc:mysql://%s:%d/%s?useSSL=false&allowPublicKeyRetrieval=true&rewriteBatchedStatements=true",
                host, getPort(), database);
            case "mariadb" -> String.format(
                "jdbc:mariadb://%s:%d/%s?useSSL=false&allowPublicKeyRetrieval=true",
                host, getPort(), database);
            case "postgresql" -> String.format("jdbc:postgresql://%s:%d/%s", host, getPort(), database);
            case "kingbase" -> String.format("jdbc:kingbase8://%s:%d/%s", host, getPort(), database);
            default -> throw new IllegalArgumentException("Unsupported DB type: " + type);
        };
    }

    /** Returns true for MySQL-family databases (MySQL and MariaDB). */
    public boolean isMysqlFamily() {
        return "mysql".equals(type) || "mariadb".equals(type);
    }

    /** Returns true for PostgreSQL-family databases (PostgreSQL and KingbaseES). */
    public boolean isPgFamily() {
        return "postgresql".equals(type) || "kingbase".equals(type);
    }
}
