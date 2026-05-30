package engine.mvcc;

public record Revision(long revision) implements Comparable<Revision>{

    public static final Revision ZERO = new Revision(0);

    public Revision next() {
        return new Revision(revision + 1);
    }

    public boolean isNewerThan(Revision other) {
        return revision > other.revision;
    }

    @Override
    public int compareTo(Revision other) {
        return Long.compare(revision, other.revision);
    }

    @Override
    public String toString() {
        return Long.toString(revision);
    }
}
