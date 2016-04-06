package proj.mapreduce.utils;

public class KeyPair<L,R> {

	private final L left;
	private final R right;

	public KeyPair(L left, R right) {
		this.left = left;
		this.right = right;
	}

	public L getLeft() { return left; }
	public R getRight() { return right; }

	@Override
	public int hashCode() { return left.hashCode() ^ right.hashCode(); }

	@Override
	public boolean equals(Object o) {
		if (!(o instanceof KeyPair)) return false;
		KeyPair pairo = (KeyPair) o;
		return this.left.equals(pairo.getLeft()) &&
				this.right.equals(pairo.getRight());
	}

}