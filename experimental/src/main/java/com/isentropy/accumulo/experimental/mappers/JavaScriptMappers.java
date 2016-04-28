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
package com.isentropy.accumulo.experimental.mappers;

import java.util.HashMap;
import java.util.Map;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;

import com.isentropy.accumulo.collections.AccumuloSortedMapBase;
import com.isentropy.accumulo.collections.DerivedMapper;
import com.isentropy.accumulo.experimental.iterators.*;
import com.isentropy.accumulo.collections.io.SerDe;

/**
 * These class contains example transformations and filters that use javascript. 
 * WARNING: there is no security checking in the server-side javascript execution. 
 * This is experimental!
 *
 */

public class JavaScriptMappers {
	protected static class JavaScriptFilterMapper implements DerivedMapper{
		private String script;
		private SerDe derivedMapSerde=null;

		public JavaScriptFilterMapper(String jscript){
			script = jscript;
		}
		public JavaScriptFilterMapper(String jscript, SerDe derivedMapSerde){
			script = jscript;
			this.derivedMapSerde = derivedMapSerde;
		}
		@Override
		public Class<? extends SortedKeyValueIterator<Key, Value>> getIterator() {
			return ScriptFilter.class;
		}

		@Override
		public Map<String, String> getIteratorOptions() {
			Map<String,String> itcfg = new HashMap<String,String>();
			itcfg.put(ScriptFilter.OPT_SCRIPTING_LANGUAGE,"JavaScript");
			itcfg.put(ScriptFilter.OPT_SCRIPT,script);			
			return itcfg;
		}

		@Override
		public SerDe getDerivedMapValueSerde() {
			// if null use same value serde as underlying map
			return derivedMapSerde;
		}
	}
	
	protected static class JavaScriptTransformMapper implements DerivedMapper{
		private String script;
		private SerDe derivedMapSerde=null;
		public JavaScriptTransformMapper(String jscript){
			script = jscript;
		}
		public JavaScriptTransformMapper(String jscript, SerDe derivedMapSerde){
			script = jscript;
			this.derivedMapSerde = derivedMapSerde;
		}
		@Override
		public Class<? extends SortedKeyValueIterator<Key, Value>> getIterator() {
			return ScriptTransformingIterator.class;
		}

		@Override
		public Map<String, String> getIteratorOptions() {
			Map<String,String> itcfg = new HashMap<String,String>();
			itcfg.put(ScriptTransformingIterator.OPT_SCRIPTING_LANGUAGE,"JavaScript");
			itcfg.put(ScriptTransformingIterator.OPT_SCRIPT,script);
			return itcfg;
		}

		@Override
		public SerDe getDerivedMapValueSerde() {
			// if null use same value serde as underlying map
			return derivedMapSerde;
		}
	}
	
	
	public static DerivedMapper jsFilter(String script){
		return new JavaScriptFilterMapper(script);
	}
	public static DerivedMapper jsTransform(String script){
		return new JavaScriptTransformMapper(script);
	}

}
