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

package com.isentropy.accumulo.util;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Map;
import java.util.Random;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.mock.MockInstance;
import org.apache.accumulo.core.client.security.tokens.AuthenticationToken;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.isentropy.accumulo.iterators.LongCountAggregateIterator;

public class Util {
	public static Logger log = LoggerFactory.getLogger(Util.class);

	final protected static char[] hexArray = "0123456789ABCDEF".toCharArray();

	
	public static Connector getMockConnector() throws AccumuloException, AccumuloSecurityException{
		AuthenticationToken token = new PasswordToken();
		Connector c = new MockInstance().getConnector("root", token);
		return c;
	}

	/**
	 * 
	 * returns fraction*FFFF...FF (hashlenth bytes), represented in hex
	 * 0th byte is most significant
	 * 
	 * @param hashlength
	 * @param fraction
	 * @return
	 */

	public static byte[] hashPoint(int hashlength,double fraction){
		int ones =  0xff;
		byte[] rslt = new byte[hashlength];
		double remainder=0;
		for(int i=0;i<rslt.length;i++){
			double product = (fraction* ones)+(remainder*256);
			int intpart = (int) product;
			remainder = (product - intpart);
			rslt[i]= (byte) intpart;
		}
		return rslt;
	}

	public static String bytesToHex(byte[] bytes) {
	    char[] hexChars = new char[bytes.length * 2];
	    for ( int j = 0; j < bytes.length; j++ ) {
	        int v = bytes[j] & 0xFF;
	        hexChars[j * 2] = hexArray[v >>> 4];
	        hexChars[j * 2 + 1] = hexArray[v & 0x0F];
	    }
	    return new String(hexChars);
	}
	public static String randomHexString(int n){
		return bytesToHex(randomBytes(n));
	}
	public static byte[] randomBytes(int n){
		Random r = new Random();
		byte[] b = new byte[n];
		r.nextBytes(b);
		return b;
	}
	/*
	public static void main(String[] args){
		int len = 16;
		Random r = new Random();
		for(int i=0;i<100;i++){
			float f = r.nextFloat();
			f=.00001f;
			System.out.println(f+"\n"+bytesToHex(hashPoint(16,f)));
		}
	}
	*/

}
