/*
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
package com.facebook.presto.client;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Objects;
import java.util.Set;

import static java.util.Objects.requireNonNull;

public class CoordinatorInfo
{
    private String environment;
    private Long updateTime;
    private Set<Object> internalNodes;

    @JsonCreator
    public CoordinatorInfo(@JsonProperty("environment") String environment, @JsonProperty("updateTime") Long updateTime)
    {
        requireNonNull(environment, "environment is null");
        requireNonNull(updateTime, "updateTime is null");
        this.environment = environment;
        this.updateTime = updateTime;
    }

    @JsonProperty
    public Long getUpdateTime()
    {
        return updateTime;
    }

    @JsonProperty
    public Set<Object> getInternalNodes()
    {
        return internalNodes;
    }

    @JsonProperty
    public String getEnvironment()
    {
        return environment;
    }

    public void setEnvironment(String environment)
    {
        this.environment = environment;
    }

    public void setUpdateTime(Long updateTime)
    {
        this.updateTime = updateTime;
    }

    public void setInternalNodes(Set<Object> internalNodes)
    {
        this.internalNodes = internalNodes;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        CoordinatorInfo that = (CoordinatorInfo) o;
        return environment.equals(that.environment) &&
                internalNodes.equals(that.internalNodes);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(environment, internalNodes);
    }

    @Override
    public String toString()
    {
        return "CoordinatorInfo{" +
                ", environment='" + environment + '\'' +
                ", updateTime=" + updateTime +
                ", internalNodes=" + internalNodes +
                '}';
    }
}
