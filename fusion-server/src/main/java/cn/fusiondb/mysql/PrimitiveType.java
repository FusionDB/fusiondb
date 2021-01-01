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

public enum PrimitiveType {
    BOOL("BOOLEAN"),
    DOUBLE("DOUBLE"),
    FLOAT("FLOAT"),
    INT("INT"),
    LONG("LONG"),
    SHORT("SHORT"),
    STRING("STRING"),
    TIMESTAMP("TIMESTAMP"),
    DATE("DATE"),
    TIME("TIME"),
    DECIMAL("DECIMAL"),
    BING_TILE("BING_TILE"),
    VARCHAR("VARCHAR");


	final String description;

	private PrimitiveType(String description) {
		this.description = description;
	}


	// TODO: Add Mysql Type to it's private field
    public MysqlColType toMysqlType() {
        switch (this) {
            // MySQL use Tinyint(1) to represent boolean
            case BOOL:
                return MysqlColType.MYSQL_TYPE_TINY;
            case DOUBLE:
                return MysqlColType.MYSQL_TYPE_DOUBLE;
            case FLOAT:
                return MysqlColType.MYSQL_TYPE_FLOAT;
            case INT:
                return MysqlColType.MYSQL_TYPE_INT24;
            case LONG:
                return MysqlColType.MYSQL_TYPE_LONG;
            case SHORT:
                return MysqlColType.MYSQL_TYPE_SHORT;
            case STRING:
                return MysqlColType.MYSQL_TYPE_STRING;
            case TIMESTAMP:
                return MysqlColType.MYSQL_TYPE_TIMESTAMP;
            case DATE:
                return MysqlColType.MYSQL_TYPE_DATE;
            case TIME:
                return MysqlColType.MYSQL_TYPE_TIME;
            case DECIMAL:
                return MysqlColType.MYSQL_TYPE_DECIMAL;
            case BING_TILE:
                return MysqlColType.MYSQL_TYPE_BIT;
            case VARCHAR:
                return MysqlColType.MYSQL_TYPE_VARCHAR;
            default:
                throw new RuntimeException("Unsupported data type!");
        }
    }
}
