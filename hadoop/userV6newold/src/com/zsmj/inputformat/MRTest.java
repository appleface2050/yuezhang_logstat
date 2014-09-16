package com.zsmj.inputformat;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.InputStreamReader;

public class MRTest {
	private static void testSplit()  throws Exception{
		FileInputStream fis= new FileInputStream("src/userinit_01.txt");
		BufferedReader reader =new BufferedReader(new InputStreamReader(fis));
		String line;
		while((line =reader.readLine())!=null){
			String[] items = line.split("\t", -1);
			if (items.length == 9 ) {
				System.out.println(items[0]+","+items[8]+"as");
			}
			System.out.println(items.length);
		}
	}
}
