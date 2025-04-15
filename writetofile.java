import java.util.*;
import java.io.*;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.Level;
 
public class ExportLoggingErrorWriteToLogFile{
/****** START SET/GET METHOD, DO NOT MODIFY *****/
	protected String xmlString = "";
	public String getxmlString() {
		return xmlString;
	}
	public void setxmlString(String val) {
		xmlString = val;
	}
/****** END SET/GET METHOD, DO NOT MODIFY *****/
	public ExportLoggingErrorWriteToLogFile() {
	}
	public void writeErrorLogs(){
		org.apache.logging.log4j.Logger logger = org.apache.logging.log4j.LogManager.getLogger("error_logger");
		logger.error("demo-begin");
		logger.error(xmlString);
		logger.error("demo-end");
	}
	public void invoke() throws Exception {
/* Available Variables: DO NOT MODIFY
	In  : String xmlString
* Available Variables: DO NOT MODIFY *****/
		writeErrorLogs();
}
}