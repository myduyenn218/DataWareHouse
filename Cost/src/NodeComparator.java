import java.util.Comparator;

public class NodeComparator implements Comparator<Node> {

	@Override
	public int compare(Node o1, Node o2) {
		return (int) o1.getPathCost() - (int) o2.getPathCost();
	}

}
