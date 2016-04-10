package proj.mapreduce.utils.network;

/**
 * command enum.
 * @author raiden
 *
 */
public enum Command {
    YARN_DETECT 		("yarn:detect:x"),
    YARN_PINGNACK 		("yarn:detect:no"),
	YARN_PINGACK  		("yarn:detect:yes"),
	YARN_RUN_JOB   		("yarn:runjob:"),
	YARN_COMPLETE_JOB	("yarn:jobcomp"),
	YARN_DO_REDUCER  	("yarn:doreducer");
	
    private final String text;

    /**
     * @param text
     */
    private Command(final String text) {
        this.text = text;
    }

    @Override
    public String toString() {
        return text;
    }
}
