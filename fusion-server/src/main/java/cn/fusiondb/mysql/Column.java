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

public class Column {
	private String name;
	private PrimitiveType columnType;
	private String defaultValue;
	public Column(String name, PrimitiveType columnType, String defalutValue) {
		this.name = name;
		this.columnType = columnType;
		this.defaultValue = defalutValue;
	}
	public String getName() {
		return name;
	}
	
	public PrimitiveType getDataType() {
		return columnType;
	}
	
	public String getDefaultValue() {
		return defaultValue;
	}
}
