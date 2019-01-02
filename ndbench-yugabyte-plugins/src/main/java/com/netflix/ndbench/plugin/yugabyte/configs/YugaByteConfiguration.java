/*
 *  Copyright 2018 Netflix, Inc.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 */
package com.netflix.ndbench.plugin.yugabyte.configs;

import com.netflix.archaius.api.annotations.Configuration;
import com.netflix.archaius.api.annotations.DefaultValue;
import com.netflix.archaius.api.annotations.PropertyName;
import com.netflix.ndbench.api.plugin.common.NdBenchConstants;

@Configuration(prefix =  NdBenchConstants.PROP_NAMESPACE +  "yugabyte")
public interface YugaByteConfiguration {
    @DefaultValue("perftest")
    String getDBName();

    @DefaultValue("postgres")
    String getPostgresDBName();

    @DefaultValue("ycql")
    String getKeyspaceName();

    @DefaultValue("yugabyte_test")
    String getTableName();

    @DefaultValue("127.0.0.1")
    String getHost();

    @DefaultValue("postgres")
    String getUser();

    @DefaultValue("6379")
    Integer getRedisPort();

    @DefaultValue("9042")
    Integer getCqlPort();

    @DefaultValue("5433")
    Integer getPostgresPort();

    @DefaultValue("")
    String getPassword();

    @DefaultValue("5")
    Integer getColsPerRow();

    @DefaultValue("100")
    String getPoolSize();
}
