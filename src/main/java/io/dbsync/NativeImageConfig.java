package io.dbsync;

import io.quarkus.runtime.annotations.RegisterForReflection;

/**
 * Registers third-party JDBC driver classes for GraalVM native image reflection.
 * Without these entries, Class.forName() fails at runtime in native binaries.
 */
@RegisterForReflection(targets = {
    com.mysql.cj.jdbc.Driver.class,
    org.mariadb.jdbc.Driver.class,
    org.postgresql.Driver.class
})
public class NativeImageConfig {
}
