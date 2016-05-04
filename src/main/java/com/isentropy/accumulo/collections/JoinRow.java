/* 
Accumulo Collections
Copyright 2016 Isentropy LLC
Written by Jonathan Wolff <jwolff@isentropy.com>
Isentropy specializes in big data and quantitative programming consulting,
with particular expertise in Accumulo development and installation. 
More info at http://isentropy.com.


Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package com.isentropy.accumulo.collections;

public class JoinRow {

	private Object untransformedKey;
	private Object transformedKey;
	private Object[] values;
	
	public JoinRow() {
	}
	public JoinRow(Object untransformedKey,Object transformedKey,Object[] values) {
		this.setTransformedKey(transformedKey);
		this.setUntransformedKey(untransformedKey);
		this.setValues(values);
	}


	public Object getUntransformedKey() {
		return untransformedKey;
	}

	public void setUntransformedKey(Object untransformedKey) {
		this.untransformedKey = untransformedKey;
	}

	public Object getTransformedKey() {
		return transformedKey;
	}

	public void setTransformedKey(Object transformedKey) {
		this.transformedKey = transformedKey;
	}

	public Object[] getValues() {
		return values;
	}

	public void setValues(Object[] values) {
		this.values = values;
	}
	@Override
	public String toString(){
		StringBuilder sb = new StringBuilder();
		sb.append("k = ("+untransformedKey+","+transformedKey+") ");
		
		sb.append("values = [");
		for(int i=0;i<values.length;i++){
			if(i > 0)
				sb.append(",");
			sb.append(values[i]);
		}
		sb.append("]");
		
		return sb.toString();
	}

}
