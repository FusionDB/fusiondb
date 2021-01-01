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

package cn.fusiondb.mysql.auth;

/**
 * Created by xiliu on 2020/12/15
 */
public class AuthResult {
    private final AuthStatus authStatus;
    private final String message;

    public AuthResult(final AuthStatus authStatus, final String message) {
        this.authStatus = authStatus;
        this.message = message;
    }

    public AuthStatus getStatus() {
        return authStatus;
    }

    public String getMessage() {
        return message;
    }
}
