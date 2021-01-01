/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License. See accompanying LICENSE file.
 */

package cn.fusiondb.mysql;

import com.google.common.base.Strings;

// MySQL protocol OK packet
public class MysqlOkPacket extends MysqlPacket {
    private static final int PACKET_OK_INDICATOR = 0X00;
    private long affectRows;

    // TODO: following are not used
    private static final long LAST_INSERT_ID = 0;
    private static final int STATUS_FLAGS = 0;
    private static final int WARNINGS = 0;
    private final String infoMessage;

    public MysqlOkPacket(QueryState state) {
        infoMessage = state.getInfoMessage();
        affectRows = state.getAffectRows();
    }

    @Override
    public void writeTo(MysqlSerializer serializer) {
        // used to check
        MysqlCapability capability = serializer.getCapability();

        serializer.writeInt1(PACKET_OK_INDICATOR);
        serializer.writeVInt(affectRows);

        serializer.writeVInt(LAST_INSERT_ID);
        if (capability.isProtocol41()) {
            serializer.writeInt2(STATUS_FLAGS);
            serializer.writeInt2(WARNINGS);
        } else if (capability.isTransactions()) {
            serializer.writeInt2(STATUS_FLAGS);
        }

        if (capability.isSessionTrack()) {
            serializer.writeLenEncodedString(infoMessage);
            // TODO(zhaochun): STATUS_FLAGS
            // if ((STATUS_FLAGS & MysqlStatusFlag.SERVER_SESSION_STATE_CHANGED) != 0) {
            // }
        } else {
            if (!Strings.isNullOrEmpty(infoMessage)) {
                // NOTE: in datasheet, use EOF string, but in the code, mysql use length encoded string
                serializer.writeLenEncodedString(infoMessage);
            }
        }
    }
}
