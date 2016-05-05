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
import java.util.HashMap;
import java.util.Map;

import javax.script.ScriptException;
import javax.script.SimpleBindings;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.PartialKey;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.OptionDescriber;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.iterators.OptionDescriber.IteratorOptions;
import org.apache.accumulo.core.iterators.user.TransformingIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import static com.isentropy.accumulo.collections.AccumuloSortedMapBase.OPT_KEY_SERDE;
import static com.isentropy.accumulo.collections.AccumuloSortedMapBase.OPT_VALUE_INPUT_SERDE;
import static com.isentropy.accumulo.collections.AccumuloSortedMapBase.OPT_VALUE_OUTPUT_SERDE;

import com.isentropy.accumulo.collections.AccumuloSortedMapBase;
import com.isentropy.accumulo.collections.io.SerDe;

public abstract class DeserializedTransformingIterator extends TransformingIterator implements OptionDescriber{
	public static Logger log = LoggerFactory.getLogger(DeserializedTransformingIterator.class);
	protected SerDe key_serde = null;
	protected SerDe value_input_serde = null;
	protected SerDe value_output_serde = null;
		
	@Override
	public IteratorOptions describeOptions() {
		String desc = "An iterator that wraps a AccumuloMapFilter to filter bytes on the tablet server";
		HashMap<String,String> namedOptions = new HashMap<String,String>();
		namedOptions.put(OPT_KEY_SERDE, "serde for input keys");
		namedOptions.put(OPT_VALUE_INPUT_SERDE, "serde for input values");
		namedOptions.put(OPT_VALUE_OUTPUT_SERDE, "serde for output values");
		return new IteratorOptions(getClass().getSimpleName(), desc, namedOptions, null);
	}

	@Override
	public boolean validateOptions(Map<String,String> options) {
		String o;
		if((o = options.get(OPT_KEY_SERDE)) != null){
			try{
				SerDe s= (SerDe) Class.forName(o).newInstance();
			}
			catch(Exception e){
				log.error("Error instantiating SerDe "+o+": "+e.getMessage());
				return false;
			}
		}
		else{
			return false;
		}
		if((o = options.get(OPT_VALUE_INPUT_SERDE)) != null){
			try{
				SerDe s= (SerDe) Class.forName(o).newInstance();
			}
			catch(Exception e){
				log.error("Error instantiating SerDe "+o+": "+e.getMessage());
				return false;
			}
		}
		else{
			return false;
		}
		if((o = options.get(OPT_VALUE_OUTPUT_SERDE)) != null){
			try{
				SerDe s= (SerDe) Class.forName(o).newInstance();
			}
			catch(Exception e){
				log.error("Error instantiating SerDe "+o+": "+e.getMessage());
				return false;
			}
		}
		return true;
	}
	@Override
	public void init(SortedKeyValueIterator<Key,Value> source, Map<String,String> options, IteratorEnvironment env) throws IOException {
		super.init(source,options,env);
		String o;
		try{
			if((o = options.get(OPT_KEY_SERDE)) != null)
				key_serde = (SerDe) Class.forName(o).newInstance();
			if((o = options.get(OPT_VALUE_INPUT_SERDE)) != null)
				value_input_serde = (SerDe) Class.forName(o).newInstance();
			if((o = options.get(OPT_VALUE_OUTPUT_SERDE)) != null)
				value_output_serde = (SerDe) Class.forName(o).newInstance();
			if(value_output_serde == null)
				value_output_serde = value_input_serde;
		}
		catch(Exception e){
			throw new IOException(e);
		}
	}
	
	
	@Override
	protected PartialKey getKeyPrefix() {
		return PartialKey.ROW;
	}
}
