import java.io.IOException;
import java.io.PrintStream;

public abstract class Logger { 

    public enum LoggingType {
        NO_LOG, ON_TERMINAL_ONLY, ON_FILE_ONLY, ON_FILE_AND_TERMINAL;
    }
    
    protected final LoggingType loggingType;
    protected PrintStream ps;

    protected Logger(LoggingType loggingType) {
        this.loggingType = loggingType;
    }
}
