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
import java.nio.charset.StandardCharsets;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;

import com.isentropy.accumulo.iterators.AggregateIterator.KeyValue;

public class LongCountAggregateIterator extends AggregateIterator{

	@Override
	protected KeyValue aggregate() throws IOException{
		long count=0;
		Key k = null;
		while(getSource().hasTop()){
			k = getSource().getTopKey();
			Value v = getSource().getTopValue();
			count++;
			getSource().next();
		}
		return new KeyValue(k,new Value(Long.toString(count).getBytes(StandardCharsets.UTF_8)));
	}

	@Override
	protected byte[] getAggregateColqual(){
		return "count".getBytes(StandardCharsets.UTF_8);
	}

}
