package kdp.keywords;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

public class Counter<T> extends HashMap<T, Double> {
	private static final long serialVersionUID = 1L;

	public void increment(T key, double amount) {
		if (!containsKey(key)) {
			put(key, 0.0);
		}
		put(key, get(key) + amount);
	}

	public void increment(T key) {
		increment(key, 1.0);
	}

	public T maxKey() {
		Entry<T, Double> maxEntry = max();
		if(maxEntry != null)
			return maxEntry.getKey();
		return null;
	}

	public Double maxValue() {
		return max().getValue();
	}

	public Map.Entry<T, Double> max() {
		Map.Entry<T, Double> max = null;
		for (Map.Entry<T, Double> entry : entrySet()) {
			if (max == null || entry.getValue() > max.getValue()) {
				max = entry;
			}
		}
		return max;
	}
}
