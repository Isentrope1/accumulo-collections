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


/**
 * an iterator for use with AccumuloSortedMap.importAll()
 * that reads utf8 tsv files and passes column 0 as string key and column 1 as string value.
 */
package com.isentropy.accumulo.util;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.AbstractMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TsvInputStreamIterator implements Iterator<Map.Entry<String,String>>{
	public static Logger log = LoggerFactory.getLogger(TsvInputStreamIterator.class);

	private InputStream input;
	private BufferedReader br;
	private String nextLine=null;

	public TsvInputStreamIterator(String filename) throws IOException{
		init(new FileInputStream(filename));
	}
	public TsvInputStreamIterator(File file) throws IOException{
		init(new FileInputStream(file));
	}
	public TsvInputStreamIterator(InputStream is) throws IOException{
		init(is);
	}
	protected void init(InputStream is) throws IOException{
		input = is;		
		br = new BufferedReader(new InputStreamReader(input, StandardCharsets.UTF_8));
		readLine();
	}
	protected void readLine() throws IOException{
		nextLine = br.readLine();
		//ignore blank lines
		if(nextLine != null && nextLine.trim().equals(""))
			readLine();
	}
	
	@Override
	public boolean hasNext() {
		return nextLine != null;
	}

	@Override
	public Entry<String, String> next() {
		try {
			String[] fields = nextLine.split("\\t");
			Entry e = new AbstractMap.SimpleEntry<String,String>(fields[0],fields[1]);
			readLine();
			return e;
		} catch (IOException e) {
			log.error(e.getMessage());
			throw new RuntimeException(e);
		}
	}

}
