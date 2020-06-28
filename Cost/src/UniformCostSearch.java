import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.PriorityQueue;

public class UniformCostSearch implements ISearchAlgo {

	@Override
	public void execute(Node tree) {

		PriorityQueue<Node> frontier = new PriorityQueue<Node>(new NodeComparator());
		frontier.add(tree);
		ArrayList<Node> explprered = new ArrayList<Node>();
		System.out.println("start");

		while (!frontier.isEmpty()) {
			Node node = frontier.poll();
			System.out.print(node.getLabel() + "\t");
			explprered.add(node);

			List<Node> childNode = node.getChildrenNodes();
			Collections.sort(childNode);

			for (Node child : childNode) {
				if (!explprered.contains(child) && !frontier.contains(child)) {
					frontier.add(child);
				}
			}

		}
	}

	@Override
	public Node execute(Node tree, String goal) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Node execute(Node tree, String start, String goal) {
		// TODO Auto-generated method stub
		return null;
	}

}
