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
package com.isentropy.accumulo.collections.transform;

import java.util.AbstractMap;
import java.util.Map;
import java.util.Map.Entry;

public class LookupValue<fromKeyType,toKeyType,fromValueType,toValueType> implements KeyValueTransformer<fromKeyType,toKeyType,fromValueType,toValueType> {
	private Map<fromKeyType,toValueType> lookup;
	public LookupValue(Map<fromKeyType,toValueType> m) {
		lookup = m;
	}
	@Override
	public Entry<toKeyType, toValueType> transformKeyValue(fromKeyType fk,
			fromValueType fv) {
		return new AbstractMap.SimpleEntry<toKeyType, toValueType>((toKeyType) fk, lookup.get(fk));
	}

}