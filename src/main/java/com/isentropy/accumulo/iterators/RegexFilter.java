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


package com.isentropy.accumulo.iterators;

import java.io.IOException;
import java.util.Map;
import java.util.regex.Pattern;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;

public class RegexFilter extends DeserializedFilter{
	public static final String OPT_KEYREGEX="keyregex";
	public static final String OPT_VALUEREGEX="valueregex";
	private Pattern keyRegex=null, valueRegex=null;
	@Override
	public void init(SortedKeyValueIterator<Key,Value> source, Map<String,String> options, IteratorEnvironment env) throws IOException {
		super.init(source,options,env);
		String s = options.get(OPT_KEYREGEX);
		if(s != null)
			keyRegex = Pattern.compile(s);
		s = options.get(OPT_VALUEREGEX);
		if(s != null)
			valueRegex = Pattern.compile(s);
		
	}
	@Override
	protected boolean allow(Object key, Object value) {
		return (keyRegex == null || keyRegex.matcher(key.toString()).find()) &&
				(valueRegex == null || valueRegex.matcher(value.toString()).find());
	}

}
