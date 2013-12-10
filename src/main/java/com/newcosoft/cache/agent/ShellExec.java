package com.newcosoft.cache.agent;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.LineNumberReader;

public class ShellExec {

	public static String runExec(String scriptPath) {
		//./redis_slave.sh master port
		
		Process process;
		try {
			process = Runtime.getRuntime().exec(scriptPath);

			InputStreamReader ir = new InputStreamReader(
					process.getInputStream());

			LineNumberReader input = new LineNumberReader(ir);

			return input.readLine();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;
	}

}
