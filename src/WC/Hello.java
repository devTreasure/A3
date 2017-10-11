package WC;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Hello {
	public static void main (String[] args) throws java.lang.Exception
	{
		
		Pattern regex = Pattern.compile("\\(.*?\\)|(,)");
		Pattern p = Pattern.compile("\\[([^\\]]*)\\]");
		
		String str ="[,s,]";
		Matcher match = regex.matcher(str);
		
				
		if (match.find()) {
			System.out.println(match.group());
		}
	}
}
