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

import cn.fusiondb.client.QueryExecutor;
import cn.fusiondb.mysql.audit.AuditLog;
import cn.fusiondb.mysql.audit.AuditService;
import com.google.common.collect.Lists;

import java.io.IOException;
import java.util.List;
import java.util.Map;


//Do one COM_QEURY process.
//first: Parse receive byte array to statement struct.
//second: Do handle function for statement.
public class StmtExecutor {
    private final AuditLog auditLog;
	private ConnectContext context;
	private MysqlSerializer serializer;
	private String originStmt;
    QueryExecutor queryExecutor;

	public StmtExecutor(ConnectContext context, String stmt) {
		this.context = context;
		this.originStmt = stmt;
		this.serializer = context.getSerializer();
        this.queryExecutor = new QueryExecutor();
        this.auditLog = AuditService.getAuditLog();
	}

	// Execute one statement.
	// Exception:
	// IOException: talk with client failed.
	public void execute() throws Exception {
		// execute
        long startTime = System.currentTimeMillis();
        try {
            handleQueryStmt();
        } catch (Exception e) {
            throw e;
        } finally {
            long endTime = System.currentTimeMillis();
            String sourceAddrs = context.getMysqlChannel().getRemote().split(":")[0];
            auditLog.log(startTime, context.getUser(), sourceAddrs, originStmt, endTime - startTime, originStmt);
        }
	}

	// Process a select statement.
    private void handleQueryStmt() throws Exception {

        // Every time set no send flag and clean all data in buffer
        context.getMysqlChannel().reset();
        originStmt = originStmt.trim();

        if (originStmt.toLowerCase().equals("select @@version")) {
            sendSingleField("@@version", "1.0.0-SNAPSHOT");
            return;
        } else if (originStmt.toLowerCase().startsWith("select @@version_comment")) {
            sendSingleField("@@version_comment", "FusionDB (Apache License), Zero 1.0.0, Revision 108");
            return;
        } else if (originStmt.toLowerCase().equals("select database()")) {
            sendSingleField("DATABASE()", "unsupported command");
            return;
        } else if (originStmt.toLowerCase().startsWith("/* mysql-connector-java") ||
                originStmt.trim().toLowerCase().startsWith("select  @@session")) {
            sendSessionVariable();
            return;
        } else if (originStmt.toLowerCase().startsWith("show databases")) {
            // TODO: rewrite show databases command.
            originStmt = "show schemas";
        } else if (originStmt.substring(0,3).toLowerCase().startsWith("set")) {
            String[] array = originStmt.split("\\s+|\\n+", 3);
            if (!array[1].toLowerCase().equals("global")) {
                context.getState().setAffectRows(1);
                context.getState().setOk();
                return;
            }
        }

        // TODO fdb context 和 client session 需要同步 client 配置, 比如：debug catalog password 等
        context.setCatalog("minio");
        queryExecutor.run(context, originStmt);
    }

	private void sendFields(List<String> colNames, List<PrimitiveType> types)
			throws IOException {
		// sends how many columns
		serializer.reset();
		serializer.writeVInt(colNames.size());
		context.getMysqlChannel().sendOnePacket(serializer.toByteBuffer());
		// send field one by one
		for (int i = 0; i < colNames.size(); ++i) {
			serializer.reset();
			serializer.writeField(colNames.get(i), types.get(i));
			context.getMysqlChannel().sendOnePacket(serializer.toByteBuffer());
		}
		// send EOF
		serializer.reset();
		MysqlEofPacket eofPacket = new MysqlEofPacket(context.getState());
		eofPacket.writeTo(serializer);
		context.getMysqlChannel().sendOnePacket(serializer.toByteBuffer());
	}

    private void sendSingleField(String colNames, String result) throws IOException {
        List<PrimitiveType> returnTypes = Lists.newArrayList();
        returnTypes.add(PrimitiveType.STRING);
        List<String> returnColumnNames = Lists.newArrayList();
        returnColumnNames.add(colNames);
        sendFields(returnColumnNames, returnTypes);

        serializer.reset();
        serializer.writeLenEncodedString(result);
        context.getMysqlChannel().sendOnePacket(serializer.toByteBuffer());
        context.getState().setEof();
    }

    private void sendSessionVariable() throws IOException {
        Map<String, SessionVariable> sessionMap = SessionVariable.sessionMap;
        List<PrimitiveType> returnTypes = Lists.newArrayList();
        List<String> returnColumnNames = Lists.newArrayList();
        List<String> returnValues = Lists.newArrayList();
        for (String sessionKey : sessionMap.keySet()) {
            returnTypes.add(sessionMap.get(sessionKey).getType());
            returnColumnNames.add(sessionMap.get(sessionKey).getName());
            returnValues.add(sessionMap.get(sessionKey).getValue());
        }
        sendFields(returnColumnNames, returnTypes);

        serializer.reset();
        for (String value : returnValues) {
            serializer.writeLenEncodedString(value);
        }
        context.getMysqlChannel().sendOnePacket(serializer.toByteBuffer());
        context.getState().setEof();
    }

	public void cancel() {
		throw new RuntimeException("not implement");
		// TODO Auto-generated method stub
	}
}
