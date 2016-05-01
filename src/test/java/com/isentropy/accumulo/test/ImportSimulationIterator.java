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

package com.isentropy.accumulo.test;

import java.util.Iterator;
import java.util.Map.Entry;

public class ImportSimulationIterator implements Iterator<Entry>{
	long count;
	public ImportSimulationIterator(long numinserts){
		count = numinserts;
	}
	@Override
	public boolean hasNext() {
		return count > 0;
	}

	@Override
	public Entry next() {
		final long c = count--;
		return new Entry(){
			@Override
			public Object getKey() {
				return c;
			}

			@Override
			public Object getValue() {
				return c+1;
			}

			@Override
			public Object setValue(Object value) {
				// TODO Auto-generated method stub
				return null;
			}
		};
	}

}