/*
 * Copyright 2020 FusionLab, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License. See accompanying LICENSE file.
 */

package cn.fusiondb.client;

import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import java.util.concurrent.ExecutionException;

/**
 * Created by xiliu on 2020/12/9
 */
@Test(singleThreaded = true)
public class QueryExecutorTest {
    QueryExecutor queryExecutor;

    @BeforeTest
    public void before() {
        queryExecutor = new QueryExecutor();
    }

    @Test
    public void queryExecutor() throws ExecutionException {
        queryExecutor.run(null, "select * from customer limit 10");
    }
}