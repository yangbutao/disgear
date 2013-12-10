package com.newcosoft.cache;

import java.util.ArrayList;
import java.util.List;

public class Util {
	public static List<String> splitSmart(String s, char separator) {
		ArrayList<String> lst = new ArrayList<String>(4);
		int pos = 0, start = 0, end = s.length();
		char inString = 0;
		char ch = 0;
		while (pos < end) {
			char prevChar = ch;
			ch = s.charAt(pos++);
			if (ch == '\\') { 
				pos++;
			} else if (inString != 0 && ch == inString) {
				inString = 0;
			} else if (ch == '\'' || ch == '"') {

				if (!Character.isLetterOrDigit(prevChar)) {
					inString = ch;
				}
			} else if (ch == separator && inString == 0) {
				lst.add(s.substring(start, pos - 1));
				start = pos;
			}
		}
		if (start < end) {
			lst.add(s.substring(start, end));
		}

		

		return lst;
	}

	public static List<String> splitSmart(String s, String separator,
			boolean decode) {
		ArrayList<String> lst = new ArrayList<String>(2);
		StringBuilder sb = new StringBuilder();
		int pos = 0, end = s.length();
		while (pos < end) {
			if (s.startsWith(separator, pos)) {
				if (sb.length() > 0) {
					lst.add(sb.toString());
					sb = new StringBuilder();
				}
				pos += separator.length();
				continue;
			}

			char ch = s.charAt(pos++);
			if (ch == '\\') {
				if (!decode)
					sb.append(ch);
				if (pos >= end)
					break; // ERROR, or let it go?
				ch = s.charAt(pos++);
				if (decode) {
					switch (ch) {
					case 'n':
						ch = '\n';
						break;
					case 't':
						ch = '\t';
						break;
					case 'r':
						ch = '\r';
						break;
					case 'b':
						ch = '\b';
						break;
					case 'f':
						ch = '\f';
						break;
					}
				}
			}

			sb.append(ch);
		}

		if (sb.length() > 0) {
			lst.add(sb.toString());
		}

		return lst;
	}

}
