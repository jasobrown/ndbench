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
package com.netflix.ndbench.plugin.aws_sql.configs;

import com.netflix.archaius.api.annotations.Configuration;
import com.netflix.archaius.api.annotations.DefaultValue;
import com.netflix.archaius.api.annotations.PropertyName;
import com.netflix.ndbench.api.plugin.common.NdBenchConstants;

@Configuration(prefix =  NdBenchConstants.PROP_NAMESPACE +  "awssql")
public interface AwsSqlConfiguration {
    @DefaultValue("jasobrown_ndbench")
    String getDBName();

    @DefaultValue("test")
    String getTableName();

    @DefaultValue("jasobrown")
    String getUser();

    @DefaultValue("buymydatabass")
    String getPassword();

    @DefaultValue("jasobrown-ndbench-cluster.cluster-ci2pfveuge6m.us-east-1.rds.amazonaws.com:3306")
    String getReadWriteUrl();

    @DefaultValue("jasobrown-ndbench-cluster.cluster-ro-ci2pfveuge6m.us-east-1.rds.amazonaws.com:3306")
    String getReadOnlyUrls();

    @DefaultValue("5")
    Integer getColsPerRow();
}