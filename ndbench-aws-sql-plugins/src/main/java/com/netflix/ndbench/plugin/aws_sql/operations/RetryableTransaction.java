package com.netflix.ndbench.plugin.aws_sql.operations;

import java.sql.Connection;
import java.sql.SQLException;

public interface RetryableTransaction
{
    void run(Connection conn) throws SQLException;
}
