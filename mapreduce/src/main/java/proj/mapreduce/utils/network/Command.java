package proj.mapreduce.utils.network;

public enum Command {
    YARN_DETECT 	("yarn:detect:x"),
    YARN_PINGNACK 	("yarn:detect:no"),
	YARN_PINGACK  	("yarn:detect:yes");

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
