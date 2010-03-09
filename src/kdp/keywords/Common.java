package kdp.keywords;

public class Common {

	static String[] tokens(String text) {
		return text.toLowerCase().split("[^a-z]+");
	}

}
