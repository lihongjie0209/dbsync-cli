package io.dbsync;

import io.quarkus.runtime.annotations.RegisterForReflection;

/**
 * Registers third-party JDBC driver classes for GraalVM native image reflection
 * using string-based classNames to avoid compile-time class loading conflicts.
 */
@RegisterForReflection(classNames = {
    "com.mysql.cj.jdbc.Driver",
    "com.mysql.cj.jdbc.NonRegisteringDriver",
    "org.mariadb.jdbc.Driver",
    "org.postgresql.Driver"
})
public class NativeImageConfig {
}
